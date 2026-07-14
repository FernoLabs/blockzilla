#!/usr/bin/env python3
import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

DEFAULT_PART_SIZE = 128 * 1024 * 1024
MULTIPART_THRESHOLD = 64 * 1024 * 1024
# Bounded recorder generations are at most 384 MiB. Keep every generation
# object on one PUT so the returned Backblaze version ID can be pinned directly.
IMMUTABLE_GENERATION_SINGLE_PUT_LIMIT = 512 * 1024 * 1024
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
SINGLE_PUT_ETAG_RE = re.compile(r"^[0-9a-f]{32}$")
API_ERROR_CODE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
GENERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
GENERATION_MANIFEST_SCHEMA_VERSION = 1
GENERATION_COMMIT_SCHEMA_VERSION = 1
GENERATION_RECEIPT_SCHEMA_VERSION = 1
READ_CHUNK_SIZE = 1024 * 1024
MAX_CREDENTIALS_FILE_BYTES = 64 * 1024
MAX_VERSION_ID_BYTES = 1024
MAX_B2_BUCKET_ID_BYTES = 1024
MAX_API_ERROR_BODY_BYTES = 16 * 1024
MAX_EXACT_KEY_VERSION_PAGES = 8
B2_EXACT_KEY_PAGE_SIZE = 100
MAX_GENERATION_VERSION_PAGES = 64
B2_GENERATION_VERSION_PAGE_SIZE = 1000
B2_AUTHORIZE_ACCOUNT_URL = "https://api.backblazeb2.com/b2api/v4/b2_authorize_account"
B2_NATIVE_API_VERSION = "v4"
B2_NATIVE_CONNECT_TIMEOUT_SECS = 10
B2_NATIVE_READ_TIMEOUT_SECS = 60
B2_ACCOUNT_USAGE_SCHEMA_VERSION = 1
BACKBLAZE_CAPACITY_EXIT_STATUSES = {
    "download_cap_exceeded": 20,
    "transaction_cap_exceeded": 21,
    # Backblaze documents storage_cap_exceeded for v4 upload-URL requests and
    # cap_exceeded for upload requests. Both mean the configured storage cap.
    "storage_cap_exceeded": 22,
    "cap_exceeded": 22,
}
ALLOWED_CREDENTIAL_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
    "AWS_SECRET_ACCESS_KEY",
    "B2_APPLICATION_KEY",
    "B2_APPLICATION_KEY_ID",
    "B2_BUCKET",
    "B2_BUCKET_ID",
    "B2_S3_ENDPOINT",
    "B2_S3_REGION",
}


def uri_encode(value: str) -> str:
    return urllib.parse.quote(value, safe="/-_.~")


def query_encode(params):
    return "&".join(
        f"{urllib.parse.quote(str(key), safe='-_.~')}={urllib.parse.quote(str(value), safe='-_.~')}"
        for key, value in sorted(params.items())
    )


def validate_sha256(value: str, label: str) -> str:
    normalized = value.lower()
    if not SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be exactly 64 hexadecimal characters")
    return normalized


def validate_generation_id(value: str) -> str:
    if not GENERATION_ID_RE.fullmatch(value):
        raise ValueError(
            "generation ID must be 1-128 safe ASCII characters and start with an alphanumeric"
        )
    return value


def validate_version_id(value: str, label: str = "version ID") -> str:
    if not value or len(value.encode("utf-8")) > MAX_VERSION_ID_BYTES:
        raise ValueError(
            f"{label} must be non-empty and at most {MAX_VERSION_ID_BYTES} bytes"
        )
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{label} contains a control character")
    return value


def validate_object_key(value: str, label: str = "object key") -> str:
    if not value or value.startswith("/"):
        raise ValueError(f"{label} must be non-empty and relative")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{label} contains a control character")
    return value


def normalize_remote_prefix(value: str) -> str:
    normalized = value.rstrip("/")
    validate_object_key(normalized, "remote prefix")
    components = normalized.split("/")
    if any(component in ("", ".", "..") for component in components):
        raise ValueError("remote prefix contains an unsafe path component")
    return normalized


def canonical_json_bytes(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _literal_env_value(raw_value: str, line_number: int) -> str:
    value = raw_value.strip()
    if not value:
        raise ValueError(f"credentials file line {line_number} has an empty value")
    if value[0] in ("'", '"'):
        quote = value[0]
        if len(value) < 2 or value[-1] != quote:
            raise ValueError(
                f"credentials file line {line_number} has an unterminated quote"
            )
        value = value[1:-1]
        if quote in value:
            raise ValueError(
                f"credentials file line {line_number} contains an unsupported embedded quote"
            )
    elif any(char.isspace() for char in value):
        raise ValueError(
            f"credentials file line {line_number} must quote values containing whitespace"
        )
    if not value or "\x00" in value or "\r" in value or "\n" in value:
        raise ValueError(f"credentials file line {line_number} has an invalid value")
    return value


def parse_credentials_file(path: Path) -> dict[str, str]:
    path = Path(path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError(f"cannot open credentials file safely: {error}") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("credentials path must be a regular file, not a symlink")
        if metadata.st_size > MAX_CREDENTIALS_FILE_BYTES:
            raise ValueError("credentials file is unexpectedly large")
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8", closefd=False) as handle:
                contents = handle.read(MAX_CREDENTIALS_FILE_BYTES + 1)
        except (OSError, UnicodeError) as error:
            raise ValueError(f"cannot read credentials file: {error}") from error
    finally:
        os.close(descriptor)
    if len(contents.encode("utf-8")) > MAX_CREDENTIALS_FILE_BYTES:
        raise ValueError("credentials file is unexpectedly large")
    if "\x00" in contents or "\r" in contents:
        raise ValueError("credentials file contains an invalid NUL or carriage return")

    values = {}
    for line_number, raw_line in enumerate(contents.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export"):
            if len(line) == len("export") or not line[len("export")].isspace():
                raise ValueError(f"credentials file line {line_number} is malformed")
            line = line[len("export") :].lstrip()
        if "=" not in line:
            raise ValueError(f"credentials file line {line_number} is missing '='")
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if key not in ALLOWED_CREDENTIAL_KEYS:
            raise ValueError(
                f"credentials file line {line_number} uses unsupported key {key!r}"
            )
        if key in values:
            raise ValueError(f"credentials file contains duplicate key {key}")
        values[key] = _literal_env_value(raw_value, line_number)
    return values


def storage_settings(credentials_file: Path | None):
    values = (
        parse_credentials_file(credentials_file) if credentials_file else os.environ
    )
    endpoint = values.get("B2_S3_ENDPOINT") or values.get("AWS_ENDPOINT_URL")
    region = values.get("B2_S3_REGION") or values.get("AWS_DEFAULT_REGION")
    bucket = values.get("B2_BUCKET")
    access_key = values.get("AWS_ACCESS_KEY_ID") or values.get("B2_APPLICATION_KEY_ID")
    secret_key = values.get("AWS_SECRET_ACCESS_KEY") or values.get("B2_APPLICATION_KEY")
    missing = [
        name
        for name, value in [
            ("B2_S3_ENDPOINT", endpoint),
            ("B2_S3_REGION", region),
            ("B2_BUCKET", bucket),
            ("AWS_ACCESS_KEY_ID", access_key),
            ("AWS_SECRET_ACCESS_KEY", secret_key),
        ]
        if not value
    ]
    if missing:
        raise ValueError(f"missing required storage settings: {', '.join(missing)}")
    return endpoint, region, bucket, access_key, secret_key


def backblaze_native_settings(credentials_file: Path | None):
    values = (
        parse_credentials_file(credentials_file) if credentials_file else os.environ
    )
    application_key_id = values.get("B2_APPLICATION_KEY_ID") or values.get(
        "AWS_ACCESS_KEY_ID"
    )
    application_key = values.get("B2_APPLICATION_KEY") or values.get(
        "AWS_SECRET_ACCESS_KEY"
    )
    missing = [
        name
        for name, value in [
            ("B2_APPLICATION_KEY_ID", application_key_id),
            ("B2_APPLICATION_KEY", application_key),
        ]
        if not value
    ]
    if missing:
        raise ValueError(f"missing required Backblaze settings: {', '.join(missing)}")
    return application_key_id, application_key


def optional_backblaze_native_object_settings(credentials_file: Path | None):
    values = (
        parse_credentials_file(credentials_file) if credentials_file else os.environ
    )
    bucket_id = values.get("B2_BUCKET_ID")
    if not bucket_id:
        return None
    application_key_id = values.get("B2_APPLICATION_KEY_ID") or values.get(
        "AWS_ACCESS_KEY_ID"
    )
    application_key = values.get("B2_APPLICATION_KEY") or values.get(
        "AWS_SECRET_ACCESS_KEY"
    )
    missing = [
        name
        for name, value in [
            ("B2_APPLICATION_KEY_ID", application_key_id),
            ("B2_APPLICATION_KEY", application_key),
        ]
        if not value
    ]
    if missing:
        raise ValueError(f"missing required Backblaze settings: {', '.join(missing)}")
    if len(bucket_id.encode("utf-8")) > MAX_B2_BUCKET_ID_BYTES or any(
        ord(char) < 0x20 or ord(char) == 0x7F for char in bucket_id
    ):
        raise ValueError("B2_BUCKET_ID is invalid")
    return application_key_id, application_key, bucket_id


def signing_key(secret: str, date: str, region: str) -> bytes:
    key = hmac.new(("AWS4" + secret).encode(), date.encode(), hashlib.sha256).digest()
    key = hmac.new(key, region.encode(), hashlib.sha256).digest()
    key = hmac.new(key, b"s3", hashlib.sha256).digest()
    return hmac.new(key, b"aws4_request", hashlib.sha256).digest()


def _bounded_api_error_body(response) -> bytes:
    """Read at most one small API error document, never an arbitrary response."""
    content_length = response.headers.get("content-length", "")
    if content_length.isdigit() and int(content_length) > MAX_API_ERROR_BODY_BYTES:
        return b""
    try:
        cached = getattr(response, "_content", False)
        if cached is False:
            body = response.raw.read(
                MAX_API_ERROR_BODY_BYTES + 1, decode_content=True
            )
        else:
            body = cached
    except Exception:
        return b""
    if not isinstance(body, bytes) or len(body) > MAX_API_ERROR_BODY_BYTES:
        return b""
    return body


def _validated_api_error_code(value) -> str:
    if not isinstance(value, str) or not API_ERROR_CODE_RE.fullmatch(value):
        return ""
    return value


def api_error_code(response) -> str:
    """Extract only a bounded JSON/XML error code; ignore messages and bodies."""
    body = _bounded_api_error_body(response)
    if not body:
        return ""
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict):
        code = _validated_api_error_code(payload.get("code"))
        if code:
            return code
        code = _validated_api_error_code(payload.get("Code"))
        if code:
            return code
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return ""
    codes = []
    for item in list(root):
        local_name = item.tag.rsplit("}", 1)[-1] if isinstance(item.tag, str) else ""
        if local_name in {"Code", "code"}:
            code = _validated_api_error_code(item.text)
            if code:
                codes.append(code)
    if len(codes) != 1:
        return ""
    return codes[0]


class APIResponseError(RuntimeError):
    def __init__(self, operation: str, status_code: int, code: str = ""):
        self.status_code = status_code
        self.code = _validated_api_error_code(code)
        # Error bodies and human-readable messages are deliberately never copied
        # into logs. Capacity codes are fixed public identifiers and are useful
        # for incident diagnosis; unknown response-controlled strings are hidden.
        detail = (
            f" ({self.code})"
            if self.code in BACKBLAZE_CAPACITY_EXIT_STATUSES
            else ""
        )
        super().__init__(f"{operation} failed HTTP {status_code}{detail}")


class S3APIError(APIResponseError):
    pass


def backblaze_capacity_exit_status(error: BaseException) -> int:
    """Return a stable cap status only for exact typed API error codes."""
    seen = set()
    current = error
    while isinstance(current, BaseException) and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, APIResponseError):
            status = BACKBLAZE_CAPACITY_EXIT_STATUSES.get(current.code)
            if status is not None:
                return status
        current = current.__cause__ or current.__context__
    return 1


class S3Client:
    def __init__(
        self,
        endpoint: str,
        region: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        retries: int,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.region = region
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.retries = retries
        parsed_endpoint = urllib.parse.urlparse(self.endpoint)
        if (
            parsed_endpoint.scheme not in ("http", "https")
            or not parsed_endpoint.netloc
        ):
            raise ValueError("S3 endpoint must be an absolute HTTP(S) URL")
        if parsed_endpoint.scheme != "https" and parsed_endpoint.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
        }:
            raise ValueError("S3 endpoint must use HTTPS")
        if parsed_endpoint.username or parsed_endpoint.password:
            raise ValueError("S3 endpoint must not contain user information")
        if (
            parsed_endpoint.path not in ("", "/")
            or parsed_endpoint.query
            or parsed_endpoint.fragment
        ):
            raise ValueError("S3 endpoint must not contain a path, query, or fragment")
        if not bucket or "/" in bucket or any(ord(char) < 0x21 for char in bucket):
            raise ValueError("S3 bucket name is invalid")
        if retries < 0:
            raise ValueError("retry count must be non-negative")
        self.host = parsed_endpoint.netloc

    def url_path(self, key: str) -> str:
        return f"/{uri_encode(self.bucket)}/{uri_encode(key.lstrip('/'))}"

    def request(
        self,
        method: str,
        key: str,
        params=None,
        headers=None,
        body=b"",
        *,
        allowed_statuses=(),
        stream=False,
    ):
        validate_object_key(key)
        last_error = None
        for attempt in range(1, self.retries + 2):
            response = None
            try:
                response = self._request_once(
                    method,
                    key,
                    params=params,
                    headers=headers,
                    body=body,
                    stream=stream,
                )
                if (
                    response.status_code < 300
                    or response.status_code in allowed_statuses
                ):
                    if not stream:
                        # Preserve the historical non-streaming success behavior.
                        # Errors stay unread until the bounded parser below.
                        try:
                            _ = response.content
                        except requests.RequestException:
                            response.close()
                            raise
                    return response
                response_error = S3APIError(
                    f"{method} {key}",
                    response.status_code,
                    api_error_code(response),
                )
                if (
                    response_error.code in BACKBLAZE_CAPACITY_EXIT_STATUSES
                    or response.status_code not in RETRYABLE_STATUS_CODES
                ):
                    response.close()
                    raise response_error
                last_error = response_error
                response.close()
            except requests.RequestException as error:
                last_error = error

            if attempt > self.retries:
                break
            delay = min(60, 2 ** (attempt - 1))
            detail = (
                f"HTTP {response.status_code}"
                if response is not None
                else last_error.__class__.__name__
            )
            print(
                f"retry {attempt}/{self.retries} after {detail} for {method} {key}; sleep={delay}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
        if isinstance(last_error, S3APIError):
            raise last_error
        raise RuntimeError(f"{method} {key} failed after retries: {last_error}")

    def _request_once(
        self,
        method: str,
        key: str,
        params=None,
        headers=None,
        body=b"",
        *,
        stream=False,
    ):
        params = params or {}
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        now = dt.datetime.now(dt.UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest() if body else EMPTY_SHA256
        canonical_uri = self.url_path(key)
        canonical_query = query_encode(params)
        signed_headers = {
            "host": self.host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            **headers,
        }
        canonical_headers = "".join(
            f"{name}:{str(value).strip()}\n"
            for name, value in sorted(signed_headers.items())
        )
        signed_header_names = ";".join(sorted(signed_headers))
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_header_names,
                payload_hash,
            ]
        )
        credential_scope = f"{date}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        signature = hmac.new(
            signing_key(self.secret_key, date, self.region),
            string_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        request_headers = {
            **signed_headers,
            "authorization": (
                "AWS4-HMAC-SHA256 "
                f"Credential={self.access_key}/{credential_scope},"
                f"SignedHeaders={signed_header_names},Signature={signature}"
            ),
        }
        url = f"{self.endpoint}{canonical_uri}"
        if canonical_query:
            url = f"{url}?{canonical_query}"
        return requests.request(
            method,
            url,
            headers=request_headers,
            data=body,
            timeout=300,
            # Successful callers can still consume response.content/response.text,
            # while failures remain bounded by _bounded_api_error_body.
            stream=True,
        )


class B2NativeAPIError(APIResponseError):
    pass


class B2NativeClient:
    """Minimal read-only Backblaze Native API client."""

    def __init__(
        self,
        application_key_id: str,
        application_key: str,
        retries: int,
        *,
        authorize_url: str = B2_AUTHORIZE_ACCOUNT_URL,
    ):
        if not application_key_id or not application_key:
            raise ValueError("Backblaze application credentials must be non-empty")
        if retries < 0:
            raise ValueError("retry count must be non-negative")
        self.application_key_id = application_key_id
        self.application_key = application_key
        self.retries = retries
        self.authorize_url = self._validate_api_url(
            authorize_url, "Backblaze authorization URL"
        )
        self.session = requests.Session()
        self.account_id = None
        self.authorization_token = None
        self.api_url = None
        self.allowed = None

    @staticmethod
    def _validate_api_url(url: str, label: str) -> str:
        parsed = urllib.parse.urlparse(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"{label} must be an absolute HTTP(S) URL")
        if parsed.scheme != "https" and parsed.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
        }:
            raise ValueError(f"{label} must use HTTPS")
        return url.rstrip("/")

    @staticmethod
    def _error_code(response) -> str:
        return api_error_code(response)

    @staticmethod
    def _retry_delay(response, attempt: int) -> int:
        delay = min(60, 2 ** (attempt - 1))
        if response is not None:
            retry_after = response.headers.get("retry-after", "")
            if retry_after.isdigit():
                delay = min(60, max(delay, int(retry_after)))
        return delay

    def _request_json(
        self,
        method: str,
        url: str,
        operation: str,
        *,
        headers=None,
        params=None,
        json_body=None,
        auth=None,
    ):
        last_error = None
        for attempt in range(1, self.retries + 2):
            response = None
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    auth=auth,
                    timeout=(
                        B2_NATIVE_CONNECT_TIMEOUT_SECS,
                        B2_NATIVE_READ_TIMEOUT_SECS,
                    ),
                    stream=True,
                )
                if response.status_code < 300:
                    try:
                        payload = response.json()
                    except (ValueError, requests.RequestException) as error:
                        raise RuntimeError(
                            f"{operation} returned invalid JSON"
                        ) from error
                    finally:
                        response.close()
                    if not isinstance(payload, dict):
                        raise RuntimeError(
                            f"{operation} returned a non-object response"
                        )
                    return payload
                code = self._error_code(response)
                if (
                    code in BACKBLAZE_CAPACITY_EXIT_STATUSES
                    or response.status_code not in RETRYABLE_STATUS_CODES
                ):
                    status_code = response.status_code
                    response.close()
                    raise B2NativeAPIError(operation, status_code, code)
                last_error = B2NativeAPIError(operation, response.status_code, code)
                response.close()
            except B2NativeAPIError:
                raise
            except requests.RequestException as error:
                last_error = error

            if attempt > self.retries:
                break
            delay = self._retry_delay(response, attempt)
            detail = (
                f"HTTP {response.status_code}"
                if response is not None
                else last_error.__class__.__name__
            )
            print(
                f"retry {attempt}/{self.retries} after {detail} for {operation}; "
                f"sleep={delay}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
        if isinstance(last_error, B2NativeAPIError):
            raise last_error
        raise RuntimeError(f"{operation} failed after retries: {last_error}")

    def authorize(self):
        payload = self._request_json(
            "GET",
            self.authorize_url,
            "b2_authorize_account",
            auth=(self.application_key_id, self.application_key),
        )
        account_id = payload.get("accountId")
        authorization_token = payload.get("authorizationToken")
        storage_api = payload.get("apiInfo", {}).get("storageApi", {})
        api_url = storage_api.get("apiUrl")
        allowed = storage_api.get("allowed")
        if not isinstance(account_id, str) or not account_id:
            raise RuntimeError("b2_authorize_account omitted accountId")
        if not isinstance(authorization_token, str) or not authorization_token:
            raise RuntimeError("b2_authorize_account omitted authorizationToken")
        if not isinstance(api_url, str) or not api_url:
            raise RuntimeError("b2_authorize_account omitted storage API URL")
        if not isinstance(allowed, dict):
            raise RuntimeError("b2_authorize_account omitted allowed capabilities")
        self.account_id = account_id
        self.authorization_token = authorization_token
        self.api_url = self._validate_api_url(api_url, "Backblaze storage API URL")
        self.allowed = allowed
        return payload

    def require_account_wide_list_access(self):
        if self.allowed is None:
            self.authorize()
        capabilities = self.allowed.get("capabilities")
        if not isinstance(capabilities, list) or not all(
            isinstance(capability, str) for capability in capabilities
        ):
            raise RuntimeError("Backblaze capability list is malformed")
        missing = {"listBuckets", "listFiles"} - set(capabilities)
        if missing:
            raise RuntimeError(
                "Backblaze key lacks account usage capabilities: "
                + ", ".join(sorted(missing))
            )
        if (
            self.allowed.get("bucketId") is not None
            or self.allowed.get("bucketIds") is not None
            or self.allowed.get("namePrefix") is not None
        ):
            raise RuntimeError(
                "Backblaze key is bucket- or prefix-restricted; account usage is incomplete"
            )

    def require_file_version_list_access(self, bucket_id: str, key: str):
        if self.allowed is None:
            self.authorize()
        capabilities = self.allowed.get("capabilities")
        if not isinstance(capabilities, list) or not all(
            isinstance(capability, str) for capability in capabilities
        ):
            raise RuntimeError("Backblaze capability list is malformed")
        if "listFiles" not in capabilities:
            raise RuntimeError("Backblaze key lacks the listFiles capability")

        allowed_bucket_id = self.allowed.get("bucketId")
        if allowed_bucket_id is not None and (
            not isinstance(allowed_bucket_id, str)
            or not hmac.compare_digest(allowed_bucket_id, bucket_id)
        ):
            raise RuntimeError("Backblaze key does not allow the configured bucket")
        allowed_bucket_ids = self.allowed.get("bucketIds")
        if allowed_bucket_ids is not None:
            if not isinstance(allowed_bucket_ids, list) or not all(
                isinstance(item, str) for item in allowed_bucket_ids
            ):
                raise RuntimeError("Backblaze bucket restriction is malformed")
            if not any(
                hmac.compare_digest(item, bucket_id) for item in allowed_bucket_ids
            ):
                raise RuntimeError("Backblaze key does not allow the configured bucket")

        name_prefix = self.allowed.get("namePrefix")
        if name_prefix is not None and (
            not isinstance(name_prefix, str) or not key.startswith(name_prefix)
        ):
            raise RuntimeError("Backblaze key does not allow the requested object key")

    def api_request(self, operation: str, *, method="GET", params=None, json_body=None):
        if self.authorization_token is None or self.api_url is None:
            self.authorize()
        url = f"{self.api_url}/b2api/{B2_NATIVE_API_VERSION}/{operation}"
        for authorization_attempt in range(2):
            try:
                return self._request_json(
                    method,
                    url,
                    operation,
                    headers={"Authorization": self.authorization_token},
                    params=params,
                    json_body=json_body,
                )
            except B2NativeAPIError as error:
                if error.code != "expired_auth_token" or authorization_attempt > 0:
                    raise
                self.authorize()
                url = f"{self.api_url}/b2api/{B2_NATIVE_API_VERSION}/{operation}"
        raise RuntimeError(f"{operation} authorization retry failed")


class B2NativeObjectVerifier:
    """Verify immutable S3 PUTs through Class-C Native version listings."""

    def __init__(
        self, client: B2NativeClient, bucket_id: str, bucket_name: str
    ):
        if not isinstance(bucket_id, str) or not bucket_id:
            raise ValueError("B2 bucket ID must be non-empty")
        if len(bucket_id.encode("utf-8")) > MAX_B2_BUCKET_ID_BYTES or any(
            ord(char) < 0x20 or ord(char) == 0x7F for char in bucket_id
        ):
            raise ValueError("B2 bucket ID is invalid")
        if not isinstance(bucket_name, str) or not bucket_name:
            raise ValueError("B2 bucket name must be non-empty")
        self.client = client
        self.bucket_id = bucket_id
        self.bucket_name = bucket_name
        self._bucket_identity_verified = False

    def _ensure_bucket_identity(self):
        if self._bucket_identity_verified:
            return
        if self.client.allowed is None:
            self.client.authorize()
        if not isinstance(self.client.account_id, str) or not self.client.account_id:
            raise RuntimeError("b2_authorize_account omitted accountId")
        allowed_bucket_id = self.client.allowed.get("bucketId")
        allowed_bucket_name = self.client.allowed.get("bucketName")
        if allowed_bucket_id is not None:
            if not isinstance(allowed_bucket_id, str) or not hmac.compare_digest(
                allowed_bucket_id, self.bucket_id
            ):
                raise RuntimeError("Backblaze key does not allow the configured bucket")
            if allowed_bucket_name is not None:
                if not isinstance(allowed_bucket_name, str) or not hmac.compare_digest(
                    allowed_bucket_name, self.bucket_name
                ):
                    raise RuntimeError(
                        "Backblaze bucket ID does not match the configured bucket name"
                    )
                self._bucket_identity_verified = True
                return

        capabilities = self.client.allowed.get("capabilities")
        if not isinstance(capabilities, list) or "listBuckets" not in capabilities:
            raise RuntimeError(
                "Backblaze key cannot prove the configured bucket ID/name mapping"
            )
        payload = self.client.api_request(
            "b2_list_buckets",
            method="POST",
            json_body={
                "accountId": self.client.account_id,
                "bucketId": self.bucket_id,
                "bucketTypes": ["all"],
            },
        )
        buckets = _response_list(payload, "buckets", "b2_list_buckets")
        if len(buckets) != 1:
            raise RuntimeError(
                "b2_list_buckets did not return exactly the configured bucket"
            )
        bucket = buckets[0]
        if (
            not isinstance(bucket.get("accountId"), str)
            or not hmac.compare_digest(bucket["accountId"], self.client.account_id)
            or not isinstance(bucket.get("bucketId"), str)
            or not hmac.compare_digest(bucket["bucketId"], self.bucket_id)
            or not isinstance(bucket.get("bucketName"), str)
            or not hmac.compare_digest(bucket["bucketName"], self.bucket_name)
        ):
            raise RuntimeError(
                "Backblaze bucket ID does not match the configured bucket name/account"
            )
        self._bucket_identity_verified = True

    def _ensure_access(self, key: str):
        self.client.require_file_version_list_access(self.bucket_id, key)
        self._ensure_bucket_identity()

    def _validate_version_identity(self, version: dict, key: str) -> str:
        file_name = version.get("fileName")
        file_id = version.get("fileId")
        account_id = version.get("accountId")
        bucket_id = version.get("bucketId")
        if not isinstance(file_name, str) or not hmac.compare_digest(file_name, key):
            raise RuntimeError(
                "b2_list_file_versions returned a different object key"
            )
        if not isinstance(file_id, str):
            raise RuntimeError("b2_list_file_versions returned an invalid fileId")
        try:
            file_id = validate_version_id(file_id, "B2 file ID")
        except ValueError as error:
            raise RuntimeError(str(error)) from error
        if not isinstance(account_id, str) or not hmac.compare_digest(
            account_id, self.client.account_id
        ):
            raise RuntimeError(
                "b2_list_file_versions returned a different accountId"
            )
        if not isinstance(bucket_id, str) or not hmac.compare_digest(
            bucket_id, self.bucket_id
        ):
            raise RuntimeError(
                "b2_list_file_versions returned a different bucketId"
            )
        return file_id

    def _list_exact_key_versions(self, key: str) -> list[dict]:
        key = validate_object_key(key)
        self._ensure_access(key)
        next_file_name = None
        next_file_id = None
        seen_markers = set()
        seen_file_ids = set()
        exact_versions = []

        for _page_number in range(1, MAX_EXACT_KEY_VERSION_PAGES + 1):
            params = {
                "bucketId": self.bucket_id,
                "prefix": key,
                "startFileName": key if next_file_name is None else next_file_name,
                "maxFileCount": B2_EXACT_KEY_PAGE_SIZE,
            }
            if next_file_id is not None:
                params["startFileId"] = next_file_id
            payload = self.client.api_request(
                "b2_list_file_versions", params=params
            )
            passed_exact_key = False
            previous_name = None
            for version in _response_list(
                payload, "files", "b2_list_file_versions"
            ):
                file_name = version.get("fileName")
                if not isinstance(file_name, str) or not file_name:
                    raise RuntimeError(
                        "b2_list_file_versions returned an invalid fileName"
                    )
                if previous_name is not None and file_name < previous_name:
                    raise RuntimeError(
                        "b2_list_file_versions returned out-of-order file names"
                    )
                previous_name = file_name
                if file_name < key:
                    raise RuntimeError(
                        "b2_list_file_versions returned a file before the requested key"
                    )
                if file_name != key:
                    passed_exact_key = True
                    continue
                if passed_exact_key:
                    raise RuntimeError(
                        "b2_list_file_versions returned a discontiguous exact key"
                    )
                file_id = self._validate_version_identity(version, key)
                if file_id in seen_file_ids:
                    raise RuntimeError(
                        "b2_list_file_versions returned a duplicate fileId"
                    )
                seen_file_ids.add(file_id)
                exact_versions.append(version)

            following_name = payload.get("nextFileName")
            following_id = payload.get("nextFileId")
            if following_name is None and following_id is None:
                return exact_versions
            if (
                not isinstance(following_name, str)
                or not following_name
                or not isinstance(following_id, str)
                or not following_id
            ):
                raise RuntimeError(
                    "b2_list_file_versions returned malformed pagination markers"
                )
            if following_name < key:
                raise RuntimeError(
                    "b2_list_file_versions pagination moved before the requested key"
                )
            if passed_exact_key:
                if following_name <= key:
                    raise RuntimeError(
                        "b2_list_file_versions pagination contradicted key ordering"
                    )
                return exact_versions
            if following_name > key:
                return exact_versions
            marker = (following_name, following_id)
            if marker in seen_markers:
                raise RuntimeError(
                    "b2_list_file_versions pagination did not advance"
                )
            seen_markers.add(marker)
            next_file_name, next_file_id = marker

        raise RuntimeError("b2_list_file_versions exceeded the exact-key page limit")

    def _seek_exact_version(self, key: str, version_id: str) -> dict:
        key = validate_object_key(key)
        version_id = validate_version_id(version_id)
        self._ensure_access(key)
        payload = self.client.api_request(
            "b2_list_file_versions",
            params={
                "bucketId": self.bucket_id,
                "prefix": key,
                "startFileName": key,
                "startFileId": version_id,
                "maxFileCount": 1,
            },
        )
        versions = _response_list(payload, "files", "b2_list_file_versions")
        if len(versions) != 1:
            raise RuntimeError(
                f"b2_list_file_versions {key} did not return the pinned object version"
            )
        returned_id = self._validate_version_identity(versions[0], key)
        if not hmac.compare_digest(returned_id, version_id):
            raise RuntimeError(
                f"b2_list_file_versions {key} returned a different pinned version"
            )
        return versions[0]

    def _validate_versions(
        self,
        versions: list[dict],
        key: str,
        expected_size: int,
        expected_sha256: str,
        expected_sha1: str,
        expected_etag: str,
    ) -> list[str]:
        expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
        expected_sha1 = expected_sha1.lower()
        if not SHA1_RE.fullmatch(expected_sha1):
            raise ValueError("expected SHA-1 must be exactly 40 hexadecimal characters")
        expected_etag = normalize_single_put_etag(expected_etag, "expected ETag")
        validated_ids = []
        for version in versions:
            file_id = self._validate_version_identity(version, key)
            action = version.get("action")
            if action != "upload":
                raise RuntimeError(
                    f"immutable B2 key {key} contains unsupported action {action!r}"
                )
            remote_size = _nonnegative_integer(
                version.get("contentLength"),
                f"b2_list_file_versions {key} contentLength",
            )
            try:
                remote_md5 = normalize_single_put_etag(
                    version.get("contentMd5", ""),
                    f"b2_list_file_versions {key} contentMd5",
                )
            except ValueError as error:
                raise RuntimeError(str(error)) from error
            remote_sha1 = version.get("contentSha1")
            if not isinstance(remote_sha1, str):
                raise RuntimeError(
                    f"b2_list_file_versions {key} omitted contentSha1"
                )
            remote_sha1 = remote_sha1.lower()
            if SHA1_RE.fullmatch(remote_sha1):
                if not hmac.compare_digest(remote_sha1, expected_sha1):
                    raise RuntimeError(
                        f"immutable B2 key {key} has conflicting SHA-1 metadata"
                    )
            elif remote_sha1.startswith("unverified:") and SHA1_RE.fullmatch(
                remote_sha1[len("unverified:") :]
            ):
                if not hmac.compare_digest(
                    remote_sha1[len("unverified:") :], expected_sha1
                ):
                    raise RuntimeError(
                        f"immutable B2 key {key} has conflicting SHA-1 metadata"
                    )
            elif remote_sha1 != "none":
                raise RuntimeError(
                    f"b2_list_file_versions {key} returned malformed contentSha1"
                )
            file_info = version.get("fileInfo")
            if not isinstance(file_info, dict):
                raise RuntimeError(
                    f"b2_list_file_versions {key} returned malformed fileInfo"
                )
            remote_sha256 = file_info.get("sha256")
            if not isinstance(remote_sha256, str):
                raise RuntimeError(
                    f"b2_list_file_versions {key} omitted SHA-256 metadata"
                )
            try:
                remote_sha256 = validate_sha256(
                    remote_sha256, f"b2_list_file_versions {key} SHA-256"
                )
            except ValueError as error:
                raise RuntimeError(str(error)) from error
            if (
                remote_size != expected_size
                or not hmac.compare_digest(remote_sha256, expected_sha256)
                or not hmac.compare_digest(remote_md5, expected_etag)
            ):
                raise RuntimeError(
                    f"immutable B2 key {key} has conflicting object metadata"
                )
            validated_ids.append(file_id)
        return validated_ids

    def latest_exact_version(
        self,
        key: str,
        expected_size: int,
        expected_sha256: str,
        expected_sha1: str,
        expected_etag: str,
    ) -> str | None:
        versions = self._list_exact_key_versions(key)
        validated_ids = self._validate_versions(
            versions,
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )
        if not validated_ids:
            return None
        selected_id = validated_ids[0]
        selected_version = self._seek_exact_version(key, selected_id)
        self._validate_versions(
            [selected_version],
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )
        return selected_id

    def verify_exact_version(
        self,
        key: str,
        expected_size: int,
        expected_sha256: str,
        expected_sha1: str,
        version_id: str,
        expected_etag: str,
    ):
        version = self._seek_exact_version(key, version_id)
        self._validate_versions(
            [version],
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )

    def list_generation_versions(
        self, remote_prefix: str, allowed_keys: set[str]
    ) -> dict[str, list[dict]]:
        """Return one authoritative, prefix-wide snapshot of generation versions."""
        generation_prefix = normalize_remote_prefix(remote_prefix) + "/"
        if not isinstance(allowed_keys, set) or not allowed_keys:
            raise ValueError("generation snapshot requires a non-empty key set")
        for key in allowed_keys:
            validate_object_key(key)
            if not key.startswith(generation_prefix):
                raise ValueError("generation snapshot key is outside the remote prefix")

        self._ensure_access(generation_prefix)
        versions_by_key = {key: [] for key in allowed_keys}
        next_file_name = generation_prefix
        next_file_id = None
        expected_first_marker = None
        previous_name = None
        seen_file_ids = set()
        seen_markers = set()

        for _page_number in range(1, MAX_GENERATION_VERSION_PAGES + 1):
            params = {
                "bucketId": self.bucket_id,
                "prefix": generation_prefix,
                "startFileName": next_file_name,
                "maxFileCount": B2_GENERATION_VERSION_PAGE_SIZE,
            }
            if next_file_id is not None:
                params["startFileId"] = next_file_id
            payload = self.client.api_request(
                "b2_list_file_versions", params=params
            )
            versions = _response_list(payload, "files", "b2_list_file_versions")
            if expected_first_marker is not None:
                if not versions:
                    raise RuntimeError(
                        "b2_list_file_versions pagination omitted its next entry"
                    )
                first_marker = (
                    versions[0].get("fileName"),
                    versions[0].get("fileId"),
                )
                if first_marker != expected_first_marker:
                    raise RuntimeError(
                        "b2_list_file_versions pagination did not resume at its marker"
                    )

            for version in versions:
                file_name = version.get("fileName")
                if not isinstance(file_name, str) or not file_name:
                    raise RuntimeError(
                        "b2_list_file_versions returned an invalid fileName"
                    )
                if not file_name.startswith(generation_prefix):
                    raise RuntimeError(
                        "b2_list_file_versions returned a key outside the generation prefix"
                    )
                if previous_name is not None and file_name < previous_name:
                    raise RuntimeError(
                        "b2_list_file_versions returned out-of-order file names"
                    )
                previous_name = file_name
                if file_name not in allowed_keys:
                    raise RuntimeError(
                        f"immutable B2 generation contains unexpected key {file_name}"
                    )
                file_id = self._validate_version_identity(version, file_name)
                if file_id in seen_file_ids:
                    raise RuntimeError(
                        "b2_list_file_versions returned a duplicate fileId"
                    )
                seen_file_ids.add(file_id)
                action = version.get("action")
                if action != "upload":
                    raise RuntimeError(
                        f"immutable B2 key {file_name} contains unsupported action {action!r}"
                    )
                versions_by_key[file_name].append(version)

            following_name = payload.get("nextFileName")
            following_id = payload.get("nextFileId")
            if following_name is None and following_id is None:
                return versions_by_key
            if (
                not isinstance(following_name, str)
                or not following_name.startswith(generation_prefix)
                or following_name not in allowed_keys
                or not isinstance(following_id, str)
            ):
                raise RuntimeError(
                    "b2_list_file_versions returned malformed generation pagination markers"
                )
            try:
                following_id = validate_version_id(
                    following_id, "B2 generation pagination file ID"
                )
            except ValueError as error:
                raise RuntimeError(str(error)) from error
            marker = (following_name, following_id)
            if marker in seen_markers or following_id in seen_file_ids:
                raise RuntimeError(
                    "b2_list_file_versions pagination did not advance"
                )
            seen_markers.add(marker)
            next_file_name, next_file_id = marker
            expected_first_marker = marker

        raise RuntimeError(
            "b2_list_file_versions exceeded the generation-prefix page limit"
        )

    def snapshot_exact_version(
        self,
        versions_by_key: dict[str, list[dict]],
        key: str,
        expected_size: int,
        expected_sha256: str,
        expected_sha1: str,
        expected_etag: str,
        pinned_version_id: str | None = None,
    ) -> str | None:
        """Validate every snapshotted version and optionally require a pinned PUT."""
        if key not in versions_by_key:
            raise ValueError("object key is outside the generation snapshot")
        validated_ids = self._validate_versions(
            versions_by_key[key],
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            expected_etag,
        )
        if pinned_version_id is not None:
            pinned_version_id = validate_version_id(
                pinned_version_id, "pinned B2 file ID"
            )
            if not any(
                hmac.compare_digest(file_id, pinned_version_id)
                for file_id in validated_ids
            ):
                raise RuntimeError(
                    f"b2_list_file_versions {key} returned a different pinned version"
                )
        return validated_ids[0] if validated_ids else None


def _nonnegative_integer(value, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"{label} must be a non-negative integer")
    return value


def _response_list(payload: dict, key: str, operation: str) -> list:
    items = payload.get(key)
    if not isinstance(items, list):
        raise RuntimeError(f"{operation} returned a malformed {key} list")
    if not all(isinstance(item, dict) for item in items):
        raise RuntimeError(f"{operation} returned a malformed {key} entry")
    return items


def _list_unfinished_part_usage(client: B2NativeClient, bucket_id: str) -> dict:
    totals = {
        "unfinished_large_file_count": 0,
        "unfinished_large_file_page_count": 0,
        "unfinished_part_bytes": 0,
        "unfinished_part_count": 0,
        "unfinished_part_page_count": 0,
    }
    next_file_id = None
    seen_file_markers = set()
    seen_unfinished_file_ids = set()
    while True:
        params = {"bucketId": bucket_id, "maxFileCount": 100}
        if next_file_id is not None:
            params["startFileId"] = next_file_id
        payload = client.api_request("b2_list_unfinished_large_files", params=params)
        totals["unfinished_large_file_page_count"] += 1
        for unfinished in _response_list(
            payload, "files", "b2_list_unfinished_large_files"
        ):
            file_id = unfinished.get("fileId")
            if not isinstance(file_id, str) or not file_id:
                raise RuntimeError(
                    "b2_list_unfinished_large_files returned an invalid fileId"
                )
            if file_id in seen_unfinished_file_ids:
                raise RuntimeError(
                    "b2_list_unfinished_large_files returned a duplicate fileId"
                )
            seen_unfinished_file_ids.add(file_id)
            totals["unfinished_large_file_count"] += 1

            next_part_number = None
            seen_part_markers = set()
            seen_part_numbers = set()
            while True:
                part_params = {"fileId": file_id, "maxPartCount": 1000}
                if next_part_number is not None:
                    part_params["startPartNumber"] = next_part_number
                part_payload = client.api_request("b2_list_parts", params=part_params)
                totals["unfinished_part_page_count"] += 1
                for part in _response_list(part_payload, "parts", "b2_list_parts"):
                    part_number = _nonnegative_integer(
                        part.get("partNumber"), "Backblaze part number"
                    )
                    if part_number < 1 or part_number in seen_part_numbers:
                        raise RuntimeError(
                            "b2_list_parts returned an invalid or duplicate part number"
                        )
                    seen_part_numbers.add(part_number)
                    totals["unfinished_part_count"] += 1
                    totals["unfinished_part_bytes"] += _nonnegative_integer(
                        part.get("contentLength"), "Backblaze part contentLength"
                    )
                marker = part_payload.get("nextPartNumber")
                if marker is None:
                    break
                marker = _nonnegative_integer(marker, "Backblaze nextPartNumber")
                if marker < 1 or marker in seen_part_markers:
                    raise RuntimeError("b2_list_parts pagination did not advance")
                seen_part_markers.add(marker)
                next_part_number = marker

        marker = payload.get("nextFileId")
        if marker is None:
            break
        if not isinstance(marker, str) or not marker or marker in seen_file_markers:
            raise RuntimeError(
                "b2_list_unfinished_large_files pagination did not advance"
            )
        seen_file_markers.add(marker)
        next_file_id = marker
    return totals


def _list_file_version_usage(client: B2NativeClient, bucket_id: str) -> dict:
    totals = {
        "folder_entry_count": 0,
        "hide_marker_count": 0,
        "start_marker_count": 0,
        "stored_upload_bytes": 0,
        "upload_version_count": 0,
        "version_page_count": 0,
    }
    next_file_name = None
    next_file_id = None
    seen_markers = set()
    while True:
        params = {"bucketId": bucket_id, "maxFileCount": 1000}
        if next_file_name is not None:
            params["startFileName"] = next_file_name
            params["startFileId"] = next_file_id
        payload = client.api_request("b2_list_file_versions", params=params)
        totals["version_page_count"] += 1
        for version in _response_list(payload, "files", "b2_list_file_versions"):
            action = version.get("action")
            content_length = _nonnegative_integer(
                version.get("contentLength"), "Backblaze version contentLength"
            )
            if action == "upload":
                totals["upload_version_count"] += 1
                totals["stored_upload_bytes"] += content_length
            elif action == "hide":
                if content_length != 0:
                    raise RuntimeError(
                        "Backblaze hide marker has non-zero contentLength"
                    )
                totals["hide_marker_count"] += 1
            elif action == "start":
                if content_length != 0:
                    raise RuntimeError(
                        "Backblaze start marker has non-zero contentLength"
                    )
                totals["start_marker_count"] += 1
            elif action == "folder":
                if content_length != 0:
                    raise RuntimeError(
                        "Backblaze folder entry has non-zero contentLength"
                    )
                totals["folder_entry_count"] += 1
            else:
                raise RuntimeError(
                    f"b2_list_file_versions returned unsupported action {action!r}"
                )

        marker = (payload.get("nextFileName"), payload.get("nextFileId"))
        if marker == (None, None):
            break
        if (
            not isinstance(marker[0], str)
            or not isinstance(marker[1], str)
            or not marker[1]
            or marker in seen_markers
        ):
            raise RuntimeError("b2_list_file_versions pagination did not advance")
        seen_markers.add(marker)
        next_file_name, next_file_id = marker
    return totals


def b2_account_usage(client: B2NativeClient) -> dict:
    client.authorize()
    client.require_account_wide_list_access()
    payload = client.api_request(
        "b2_list_buckets",
        method="POST",
        json_body={"accountId": client.account_id, "bucketTypes": ["all"]},
    )
    buckets = _response_list(payload, "buckets", "b2_list_buckets")
    bucket_ids = []
    for bucket in buckets:
        bucket_id = bucket.get("bucketId")
        if not isinstance(bucket_id, str) or not bucket_id or bucket_id in bucket_ids:
            raise RuntimeError(
                "b2_list_buckets returned an invalid or duplicate bucketId"
            )
        bucket_ids.append(bucket_id)

    result = {
        "bucket_count": len(bucket_ids),
        "folder_entry_count": 0,
        "hide_marker_count": 0,
        "schema_version": B2_ACCOUNT_USAGE_SCHEMA_VERSION,
        "scope": "account",
        "scope_complete": True,
        "scanned_unix_secs": int(time.time()),
        "start_marker_count": 0,
        "stored_upload_bytes": 0,
        "total_stored_bytes": 0,
        "unfinished_large_file_count": 0,
        "unfinished_large_file_page_count": 0,
        "unfinished_part_bytes": 0,
        "unfinished_part_count": 0,
        "unfinished_part_page_count": 0,
        "upload_version_count": 0,
        "version_page_count": 0,
    }
    for bucket_id in bucket_ids:
        # Scan unfinished uploads before completed versions. If a multipart upload
        # completes between the two reads, it is conservatively counted twice
        # instead of being omitted from this non-transactional snapshot.
        unfinished = _list_unfinished_part_usage(client, bucket_id)
        versions = _list_file_version_usage(client, bucket_id)
        for key, value in unfinished.items():
            result[key] += value
        for key, value in versions.items():
            result[key] += value
    result["total_stored_bytes"] = (
        result["stored_upload_bytes"] + result["unfinished_part_bytes"]
    )
    return result


def xml_text(root, tag):
    for item in root.iter():
        if item.tag.endswith(tag):
            return item.text
    return None


def object_headers(content_type: str, sha256: str):
    return {
        "content-type": content_type,
        "x-amz-meta-sha256": validate_sha256(sha256, "object SHA-256"),
    }


def response_version_id(response, operation: str, key: str) -> str:
    version_id = response.headers.get("x-amz-version-id", "")
    try:
        return validate_version_id(version_id, f"{operation} {key} version ID")
    except ValueError as error:
        raise RuntimeError(str(error)) from error


def normalize_single_put_etag(value: str, label: str) -> str:
    normalized = value.strip()
    if len(normalized) >= 2 and normalized[0] == '"' and normalized[-1] == '"':
        normalized = normalized[1:-1]
    normalized = normalized.lower()
    if not SINGLE_PUT_ETAG_RE.fullmatch(normalized):
        raise ValueError(f"{label} must be a single-part 32-hexadecimal ETag")
    return normalized


def response_single_put_etag(response, operation: str, key: str) -> str:
    try:
        return normalize_single_put_etag(
            response.headers.get("etag", ""), f"{operation} {key} ETag"
        )
    except ValueError as error:
        raise RuntimeError(str(error)) from error


def single_put(
    client: S3Client,
    path: Path,
    key: str,
    content_type: str,
    sha256: str,
):
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != sha256:
        raise RuntimeError(f"local file changed while preparing {key}")
    expected_etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
    response = client.request(
        "PUT", key, headers=object_headers(content_type, sha256), body=data
    )
    try:
        version_id = response_version_id(response, "PUT", key)
        returned_etag = response_single_put_etag(response, "PUT", key)
        if not hmac.compare_digest(returned_etag, expected_etag):
            raise RuntimeError(f"PUT {key} ETag mismatch")
        return version_id, returned_etag
    finally:
        response.close()


def single_put_bytes(
    client: S3Client,
    data: bytes,
    key: str,
    content_type: str,
    sha256: str,
):
    if hashlib.sha256(data).hexdigest() != sha256:
        raise RuntimeError(f"in-memory object digest mismatch for {key}")
    expected_etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
    response = client.request(
        "PUT", key, headers=object_headers(content_type, sha256), body=data
    )
    try:
        version_id = response_version_id(response, "PUT", key)
        returned_etag = response_single_put_etag(response, "PUT", key)
        if not hmac.compare_digest(returned_etag, expected_etag):
            raise RuntimeError(f"PUT {key} ETag mismatch")
        return version_id, returned_etag
    finally:
        response.close()


def multipart_put(
    client: S3Client,
    path: Path,
    key: str,
    content_type: str,
    part_size: int,
    sha256: str,
):
    if part_size <= 0:
        raise ValueError("multipart part size must be positive")
    upload_id = None
    parts = []
    try:
        response = client.request(
            "POST",
            key,
            params={"uploads": ""},
            headers=object_headers(content_type, sha256),
        )
        upload_id = xml_text(ET.fromstring(response.text), "UploadId")
        response.close()
        if not upload_id:
            raise RuntimeError(
                f"create multipart upload did not return UploadId for {key}"
            )

        total = path.stat().st_size
        sent = 0
        part_number = 1
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(part_size)
                if not chunk:
                    break
                response = client.request(
                    "PUT",
                    key,
                    params={"partNumber": part_number, "uploadId": upload_id},
                    body=chunk,
                )
                etag = response.headers.get("ETag") or response.headers.get("etag")
                response.close()
                if not etag:
                    raise RuntimeError(f"upload part {part_number} did not return ETag")
                parts.append((part_number, etag))
                sent += len(chunk)
                print(
                    f"{key}: part={part_number} bytes={sent}/{total} pct={sent * 100 / total:.1f}",
                    file=sys.stderr,
                    flush=True,
                )
                part_number += 1

        body = (
            "<CompleteMultipartUpload>"
            + "".join(
                f"<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
                for number, etag in parts
            )
            + "</CompleteMultipartUpload>"
        ).encode()
        response = client.request(
            "POST", key, params={"uploadId": upload_id}, body=body
        )
        try:
            return response_version_id(response, "complete multipart upload", key)
        finally:
            response.close()
    except Exception:
        if upload_id:
            try:
                response = client.request("DELETE", key, params={"uploadId": upload_id})
                response.close()
            except Exception as abort_error:
                print(
                    f"warning: failed to abort multipart upload: {abort_error}",
                    file=sys.stderr,
                )
        raise


def file_digests(path: Path) -> tuple[int, str, str, str]:
    sha256_digest = hashlib.sha256()
    sha1_digest = hashlib.sha1(usedforsecurity=False)
    md5_digest = hashlib.md5(usedforsecurity=False)
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            size += len(chunk)
            sha256_digest.update(chunk)
            sha1_digest.update(chunk)
            md5_digest.update(chunk)
    return (
        size,
        sha256_digest.hexdigest(),
        sha1_digest.hexdigest(),
        md5_digest.hexdigest(),
    )


def sha256_file(path: Path) -> tuple[int, str]:
    size, sha256, _sha1, _md5 = file_digests(path)
    return size, sha256


def _head_matches(
    response,
    expected_size: int,
    expected_sha256: str,
    key: str,
    expected_version_id: str | None = None,
    expected_etag: str | None = None,
) -> str:
    try:
        remote_size = int(response.headers.get("content-length", ""))
    except ValueError as error:
        raise RuntimeError(f"HEAD {key} returned an invalid Content-Length") from error
    if remote_size != expected_size:
        raise RuntimeError(
            f"HEAD {key} size mismatch: remote={remote_size} expected={expected_size}"
        )
    remote_sha256 = response.headers.get("x-amz-meta-sha256", "").lower()
    if not hmac.compare_digest(remote_sha256, expected_sha256):
        raise RuntimeError(f"HEAD {key} SHA-256 metadata mismatch")
    version_id = response_version_id(response, "HEAD", key)
    if expected_version_id is not None and not hmac.compare_digest(
        version_id,
        validate_version_id(expected_version_id, "expected version ID"),
    ):
        raise RuntimeError(f"HEAD {key} returned a different object version")
    if expected_etag is not None:
        remote_etag = response_single_put_etag(response, "HEAD", key)
        normalized_expected_etag = normalize_single_put_etag(
            expected_etag, "expected ETag"
        )
        if not hmac.compare_digest(remote_etag, normalized_expected_etag):
            raise RuntimeError(f"HEAD {key} ETag mismatch")
    return version_id


def verify_remote_metadata(
    client: S3Client,
    key: str,
    expected_size: int,
    expected_sha256: str,
    version_id: str,
    expected_etag: str | None = None,
    metadata_verifier: B2NativeObjectVerifier | None = None,
    expected_sha1: str | None = None,
):
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    version_id = validate_version_id(version_id)
    if metadata_verifier is not None:
        if expected_etag is None or expected_sha1 is None:
            raise ValueError(
                "native metadata verification requires single-PUT MD5 and SHA-1 digests"
            )
        metadata_verifier.verify_exact_version(
            key,
            expected_size,
            expected_sha256,
            expected_sha1,
            version_id,
            expected_etag,
        )
        return
    head = client.request("HEAD", key, params={"versionId": version_id})
    try:
        _head_matches(
            head,
            expected_size,
            expected_sha256,
            key,
            version_id,
            expected_etag,
        )
    finally:
        head.close()


def verify_remote_object(
    client: S3Client,
    key: str,
    expected_size: int,
    expected_sha256: str,
    version_id: str,
    expected_etag: str | None = None,
):
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    version_id = validate_version_id(version_id)
    version_params = {"versionId": version_id}
    verify_remote_metadata(
        client,
        key,
        expected_size,
        expected_sha256,
        version_id,
        expected_etag,
    )

    response = client.request(
        "GET",
        key,
        params=version_params,
        headers={"accept-encoding": "identity"},
        stream=True,
    )
    downloaded = 0
    digest = hashlib.sha256()
    try:
        downloaded_version_id = response_version_id(response, "GET", key)
        if not hmac.compare_digest(downloaded_version_id, version_id):
            raise RuntimeError(f"GET {key} returned a different object version")
        if expected_etag is not None:
            downloaded_etag = response_single_put_etag(response, "GET", key)
            if not hmac.compare_digest(
                downloaded_etag,
                normalize_single_put_etag(expected_etag, "expected ETag"),
            ):
                raise RuntimeError(f"GET {key} ETag mismatch")
        response.raw.decode_content = False
        while True:
            chunk = response.raw.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            downloaded += len(chunk)
            digest.update(chunk)
    finally:
        response.close()
    if downloaded != expected_size:
        raise RuntimeError(
            f"GET {key} size mismatch: downloaded={downloaded} expected={expected_size}"
        )
    downloaded_sha256 = digest.hexdigest()
    if not hmac.compare_digest(downloaded_sha256, expected_sha256):
        raise RuntimeError(f"GET {key} SHA-256 mismatch")


def verify_remote_sha256(
    client: S3Client,
    key: str,
    expected_sha256: str,
    version_id: str,
) -> int:
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    version_id = validate_version_id(version_id)
    head = client.request("HEAD", key, params={"versionId": version_id})
    try:
        try:
            size = int(head.headers.get("content-length", ""))
        except ValueError as error:
            raise RuntimeError(
                f"HEAD {key} returned an invalid Content-Length"
            ) from error
        if size < 0:
            raise RuntimeError(f"HEAD {key} returned a negative Content-Length")
        remote_sha256 = head.headers.get("x-amz-meta-sha256", "").lower()
        if not hmac.compare_digest(remote_sha256, expected_sha256):
            raise RuntimeError(f"HEAD {key} SHA-256 metadata mismatch")
        returned_version_id = response_version_id(head, "HEAD", key)
        if not hmac.compare_digest(returned_version_id, version_id):
            raise RuntimeError(f"HEAD {key} returned a different object version")
    finally:
        head.close()
    verify_remote_object(client, key, size, expected_sha256, version_id)
    return size


def latest_exact_object_version(
    client: S3Client,
    key: str,
    expected_size: int,
    expected_sha256: str,
    *,
    full_readback: bool = True,
    expected_etag: str | None = None,
    metadata_verifier: B2NativeObjectVerifier | None = None,
    expected_sha1: str | None = None,
) -> str | None:
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    if metadata_verifier is not None:
        if full_readback:
            raise ValueError(
                "native metadata verification is only valid without full readback"
            )
        if expected_etag is None or expected_sha1 is None:
            raise ValueError(
                "native metadata verification requires single-PUT MD5 and SHA-1 digests"
            )
        return metadata_verifier.latest_exact_version(
            key, expected_size, expected_sha256, expected_sha1, expected_etag
        )
    response = client.request("HEAD", key, allowed_statuses=(404,))
    try:
        if response.status_code == 404:
            return None
        version_id = _head_matches(
            response,
            expected_size,
            expected_sha256,
            key,
            expected_etag=expected_etag,
        )
    finally:
        response.close()
    if full_readback:
        verify_remote_object(
            client,
            key,
            expected_size,
            expected_sha256,
            version_id,
            expected_etag,
        )
    else:
        verify_remote_metadata(
            client,
            key,
            expected_size,
            expected_sha256,
            version_id,
            expected_etag,
        )
    return version_id


def upload_verified_file(
    client: S3Client,
    path: Path,
    key: str,
    content_type: str = "application/octet-stream",
    part_size: int = DEFAULT_PART_SIZE,
    multipart_threshold: int = MULTIPART_THRESHOLD,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    full_readback: bool = True,
    metadata_verifier: B2NativeObjectVerifier | None = None,
):
    path = Path(path)
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"upload source must be a regular file: {path}")
    measured_size, measured_sha256, measured_sha1, measured_md5 = file_digests(path)
    if expected_size is not None and measured_size != expected_size:
        raise RuntimeError(f"local file size changed before upload: {path}")
    if expected_sha256 is not None and not hmac.compare_digest(
        measured_sha256, validate_sha256(expected_sha256, "expected local SHA-256")
    ):
        raise RuntimeError(f"local file SHA-256 changed before upload: {path}")
    size = measured_size
    sha256 = measured_sha256
    single_part = size < multipart_threshold
    if not full_readback and not single_part:
        raise ValueError(
            "metadata-only verification requires a single-PUT object with a plain MD5 ETag"
        )
    expected_etag = measured_md5 if single_part else None

    existing_version_id = latest_exact_object_version(
        client,
        key,
        size,
        sha256,
        full_readback=full_readback,
        expected_etag=expected_etag,
        metadata_verifier=metadata_verifier,
        expected_sha1=measured_sha1,
    )
    if existing_version_id is not None:
        return {
            "key": key,
            "size": size,
            "sha256": sha256,
            "version_id": existing_version_id,
            "already_present": True,
        }
    if size >= multipart_threshold:
        version_id = multipart_put(client, path, key, content_type, part_size, sha256)
    else:
        version_id, returned_etag = single_put(
            client, path, key, content_type, sha256
        )
        if not hmac.compare_digest(returned_etag, expected_etag):
            raise RuntimeError(f"PUT {key} ETag mismatch")
    after = path.stat(follow_symlinks=False)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"local file changed during upload: {path}")
    if full_readback:
        verify_remote_object(
            client, key, size, sha256, version_id, expected_etag
        )
    else:
        verify_remote_metadata(
            client,
            key,
            size,
            sha256,
            version_id,
            expected_etag,
            metadata_verifier,
            measured_sha1,
        )
    if latest_exact_object_version(
        client,
        key,
        size,
        sha256,
        full_readback=full_readback,
        expected_etag=expected_etag,
        metadata_verifier=metadata_verifier,
        expected_sha1=measured_sha1,
    ) is None:
        raise RuntimeError(f"uploaded object disappeared before publication: {key}")
    return {
        "key": key,
        "size": size,
        "sha256": sha256,
        "version_id": version_id,
        "already_present": False,
    }


def upload_verified_bytes(
    client: S3Client,
    data: bytes,
    key: str,
    content_type: str = "application/json",
    *,
    full_readback: bool = True,
    metadata_verifier: B2NativeObjectVerifier | None = None,
):
    sha256 = hashlib.sha256(data).hexdigest()
    sha1 = hashlib.sha1(data, usedforsecurity=False).hexdigest()
    expected_etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
    size = len(data)
    existing_version_id = latest_exact_object_version(
        client,
        key,
        size,
        sha256,
        full_readback=full_readback,
        expected_etag=expected_etag,
        metadata_verifier=metadata_verifier,
        expected_sha1=sha1,
    )
    if existing_version_id is not None:
        return {
            "key": key,
            "size": size,
            "sha256": sha256,
            "version_id": existing_version_id,
            "already_present": True,
        }
    version_id, returned_etag = single_put_bytes(
        client, data, key, content_type, sha256
    )
    if not hmac.compare_digest(returned_etag, expected_etag):
        raise RuntimeError(f"PUT {key} ETag mismatch")
    if full_readback:
        verify_remote_object(
            client, key, size, sha256, version_id, expected_etag
        )
    else:
        verify_remote_metadata(
            client,
            key,
            size,
            sha256,
            version_id,
            expected_etag,
            metadata_verifier,
            sha1,
        )
    if latest_exact_object_version(
        client,
        key,
        size,
        sha256,
        full_readback=full_readback,
        expected_etag=expected_etag,
        metadata_verifier=metadata_verifier,
        expected_sha1=sha1,
    ) is None:
        raise RuntimeError(f"uploaded object disappeared before publication: {key}")
    return {
        "key": key,
        "size": size,
        "sha256": sha256,
        "version_id": version_id,
        "already_present": False,
    }


def _validate_relative_generation_path(path: str):
    if not path or path.startswith("/") or "\\" in path:
        raise ValueError(f"unsafe generation path {path!r}")
    components = path.split("/")
    if any(component in ("", ".", "..") for component in components):
        raise ValueError(f"unsafe generation path {path!r}")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in path):
        raise ValueError(f"generation path contains a control character: {path!r}")


def walk_generation(root: Path):
    root = Path(root).absolute()
    root_metadata = root.lstat()
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise ValueError("generation root must be a directory, not a symlink")

    files = []

    def visit(directory: Path):
        with os.scandir(directory) as entries:
            for entry in entries:
                path = Path(entry.path)
                metadata = entry.stat(follow_symlinks=False)
                relative = path.relative_to(root).as_posix()
                _validate_relative_generation_path(relative)
                if stat.S_ISLNK(metadata.st_mode):
                    raise ValueError(f"generation contains a symlink: {relative}")
                if stat.S_ISDIR(metadata.st_mode):
                    visit(path)
                elif stat.S_ISREG(metadata.st_mode):
                    files.append((relative, path))
                else:
                    raise ValueError(
                        f"generation contains a non-regular entry: {relative}"
                    )

    visit(root)
    files.sort(key=lambda item: item[0].encode("utf-8"))
    if not files:
        raise ValueError("generation contains no regular files")
    return files


@contextlib.contextmanager
def lock_stopped_generation(root: Path):
    initial_files = walk_generation(root)
    relative_paths = {relative for relative, _path in initial_files}
    missing_required = {"identity.json", "raw-blocks.jsonl"} - relative_paths
    if missing_required:
        raise ValueError(
            "generation is missing required file(s): "
            + ", ".join(sorted(missing_required))
        )
    if not any(relative.endswith(".wal") for relative in relative_paths):
        raise ValueError("generation contains no WAL segment")
    lock_paths = [
        path
        for relative, path in initial_files
        if relative.split("/")[-1] == "writer.lock"
    ]
    if len(lock_paths) != 1:
        raise ValueError(
            f"generation must contain exactly one WAL writer.lock, found {len(lock_paths)}"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(lock_paths[0], flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("generation writer.lock is not a regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                "generation WAL is still locked by an active writer"
            ) from error
        yield
    finally:
        os.close(descriptor)


def build_generation_file_records(
    generation_dir: Path,
    remote_prefix: str,
):
    remote_prefix = normalize_remote_prefix(remote_prefix)
    files = []
    total_bytes = 0
    for relative, path in walk_generation(generation_dir):
        size, sha256 = sha256_file(path)
        total_bytes += size
        files.append(
            {
                "object_key": f"{remote_prefix}/files/{relative}",
                "path": relative,
                "sha256": sha256,
                "size": size,
            }
        )
    return files, total_bytes


def build_generation_manifest(
    generation_id: str,
    files: list[dict],
    file_version_ids: dict[str, str],
    total_bytes: int,
    predecessor_manifest_sha256: str | None = None,
):
    generation_id = validate_generation_id(generation_id)
    predecessor = (
        validate_sha256(predecessor_manifest_sha256, "predecessor manifest SHA-256")
        if predecessor_manifest_sha256
        else None
    )
    expected_paths = {record["path"] for record in files}
    if set(file_version_ids) != expected_paths:
        raise ValueError("generation file-version map does not match local files")
    versioned_files = []
    for record in files:
        versioned = dict(record)
        versioned["version_id"] = validate_version_id(
            file_version_ids[record["path"]],
            f"generation file {record['path']} version ID",
        )
        versioned_files.append(versioned)
    manifest = {
        "files": versioned_files,
        "generation_id": generation_id,
        "schema_version": GENERATION_MANIFEST_SCHEMA_VERSION,
        "total_bytes": total_bytes,
    }
    if predecessor is not None:
        manifest["predecessor_manifest_sha256"] = predecessor
    return manifest


def generation_commit_payload(
    generation_id: str,
    manifest_key: str,
    manifest_sha256: str,
    manifest_version_id: str,
    file_count: int,
    total_bytes: int,
    predecessor_manifest_sha256: str | None = None,
) -> bytes:
    validate_generation_id(generation_id)
    validate_object_key(manifest_key, "manifest key")
    manifest_sha256 = validate_sha256(manifest_sha256, "manifest SHA-256")
    manifest_version_id = validate_version_id(
        manifest_version_id, "manifest version ID"
    )
    if file_count < 1 or total_bytes < 1:
        raise ValueError(
            "committed generation must contain at least one byte in one file"
        )
    commit = {
        "file_count": file_count,
        "generation_id": generation_id,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_sha256,
        "manifest_version_id": manifest_version_id,
        "schema_version": GENERATION_COMMIT_SCHEMA_VERSION,
        "total_bytes": total_bytes,
    }
    if predecessor_manifest_sha256:
        commit["predecessor_manifest_sha256"] = validate_sha256(
            predecessor_manifest_sha256,
            "predecessor manifest SHA-256",
        )
    return canonical_json_bytes(commit)


def write_receipt_atomic(path: Path, receipt: dict, generation_dir: Path):
    requested_path = Path(path).absolute()
    requested_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_parent = requested_path.parent.resolve(strict=True)
    receipt_path = receipt_parent / requested_path.name
    generation_root = Path(generation_dir).resolve(strict=True)
    try:
        common = Path(os.path.commonpath((generation_root, receipt_path)))
    except ValueError:
        common = None
    if common == generation_root:
        raise ValueError("generation receipt must be outside the generation directory")
    payload = canonical_json_bytes(receipt)
    temporary = (
        receipt_parent / f".{receipt_path.name}.tmp.{os.getpid()}.{time.time_ns()}"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        os.replace(temporary, receipt_path)
        directory_descriptor = os.open(
            receipt_parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _file_stat_identity(metadata) -> tuple[int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _bytes_metadata_spec(data: bytes) -> dict:
    return {
        "etag": hashlib.md5(data, usedforsecurity=False).hexdigest(),
        "sha1": hashlib.sha1(data, usedforsecurity=False).hexdigest(),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }


def _generation_file_metadata_specs(
    local_files: list[dict], file_by_relative: dict[str, Path]
) -> tuple[dict[str, dict], dict[str, tuple[int, int, int, int]]]:
    specs = {}
    stat_identities = {}
    for record in local_files:
        path = file_by_relative[record["path"]]
        before = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"upload source must be a regular file: {path}")
        size, sha256, sha1, etag = file_digests(path)
        after = path.stat(follow_symlinks=False)
        if _file_stat_identity(before) != _file_stat_identity(after):
            raise RuntimeError(f"local file changed while hashing: {path}")
        if size != record["size"] or not hmac.compare_digest(
            sha256, record["sha256"]
        ):
            raise RuntimeError(f"generation changed while hashing: {path}")
        specs[record["object_key"]] = {
            "etag": etag,
            "sha1": sha1,
            "sha256": sha256,
            "size": size,
        }
        stat_identities[record["path"]] = _file_stat_identity(after)
    return specs, stat_identities


def _snapshot_object_version(
    metadata_verifier: B2NativeObjectVerifier,
    snapshot: dict[str, list[dict]],
    key: str,
    spec: dict,
    pinned_version_id: str | None = None,
) -> str | None:
    return metadata_verifier.snapshot_exact_version(
        snapshot,
        key,
        spec["size"],
        spec["sha256"],
        spec["sha1"],
        spec["etag"],
        pinned_version_id,
    )


def _validate_pinned_snapshot_objects(
    metadata_verifier: B2NativeObjectVerifier,
    snapshot: dict[str, list[dict]],
    specs: dict[str, dict],
    pinned_version_ids: dict[str, str],
):
    if set(specs) != set(pinned_version_ids):
        raise ValueError("snapshot object-version map does not match expected objects")
    for key, spec in specs.items():
        _snapshot_object_version(
            metadata_verifier,
            snapshot,
            key,
            spec,
            pinned_version_ids[key],
        )


def _upload_generation_with_native_snapshot_verification(
    client: S3Client,
    generation_id: str,
    remote_prefix: str,
    local_files: list[dict],
    total_bytes: int,
    file_by_relative: dict[str, Path],
    predecessor_manifest_sha256: str | None,
    metadata_verifier: B2NativeObjectVerifier,
) -> dict:
    """Publish a generation with O(prefix pages), not O(objects), Native reads."""
    file_specs, stat_identities = _generation_file_metadata_specs(
        local_files, file_by_relative
    )
    manifest_key = f"{remote_prefix}/manifest.json"
    commit_key = f"{remote_prefix}/_COMMITTED"
    allowed_keys = set(file_specs) | {manifest_key, commit_key}

    snapshot = metadata_verifier.list_generation_versions(
        remote_prefix, allowed_keys
    )
    file_version_ids = {}
    missing_records = []
    for record in local_files:
        key = record["object_key"]
        version_id = _snapshot_object_version(
            metadata_verifier, snapshot, key, file_specs[key]
        )
        if version_id is None:
            missing_records.append(record)
        else:
            file_version_ids[record["path"]] = version_id

    if missing_records and (snapshot[manifest_key] or snapshot[commit_key]):
        raise RuntimeError(
            "immutable B2 generation has a manifest or commit before all files"
        )

    for record in missing_records:
        path = file_by_relative[record["path"]]
        key = record["object_key"]
        version_id, returned_etag = single_put(
            client,
            path,
            key,
            "application/octet-stream",
            file_specs[key]["sha256"],
        )
        if not hmac.compare_digest(returned_etag, file_specs[key]["etag"]):
            raise RuntimeError(f"PUT {key} ETag mismatch")
        after = path.stat(follow_symlinks=False)
        if _file_stat_identity(after) != stat_identities[record["path"]]:
            raise RuntimeError(f"local file changed during upload: {path}")
        file_version_ids[record["path"]] = version_id

    file_pins = {
        record["object_key"]: file_version_ids[record["path"]]
        for record in local_files
    }
    if missing_records:
        snapshot = metadata_verifier.list_generation_versions(
            remote_prefix, allowed_keys
        )
        _validate_pinned_snapshot_objects(
            metadata_verifier, snapshot, file_specs, file_pins
        )

    manifest = build_generation_manifest(
        generation_id,
        local_files,
        file_version_ids,
        total_bytes,
        predecessor_manifest_sha256,
    )
    manifest_bytes = canonical_json_bytes(manifest)
    manifest_spec = _bytes_metadata_spec(manifest_bytes)
    manifest_version_id = _snapshot_object_version(
        metadata_verifier, snapshot, manifest_key, manifest_spec
    )
    if manifest_version_id is None:
        if snapshot[commit_key]:
            raise RuntimeError(
                "immutable B2 generation has a commit without its manifest"
            )
        manifest_version_id, returned_etag = single_put_bytes(
            client,
            manifest_bytes,
            manifest_key,
            "application/json",
            manifest_spec["sha256"],
        )
        if not hmac.compare_digest(returned_etag, manifest_spec["etag"]):
            raise RuntimeError(f"PUT {manifest_key} ETag mismatch")
        snapshot = metadata_verifier.list_generation_versions(
            remote_prefix, allowed_keys
        )
        _validate_pinned_snapshot_objects(
            metadata_verifier, snapshot, file_specs, file_pins
        )
        _snapshot_object_version(
            metadata_verifier,
            snapshot,
            manifest_key,
            manifest_spec,
            manifest_version_id,
        )

    commit_bytes = generation_commit_payload(
        generation_id,
        manifest_key,
        manifest_spec["sha256"],
        manifest_version_id,
        len(local_files),
        total_bytes,
        predecessor_manifest_sha256,
    )
    commit_spec = _bytes_metadata_spec(commit_bytes)
    commit_version_id = _snapshot_object_version(
        metadata_verifier, snapshot, commit_key, commit_spec
    )
    if commit_version_id is None:
        commit_version_id, returned_etag = single_put_bytes(
            client,
            commit_bytes,
            commit_key,
            "application/json",
            commit_spec["sha256"],
        )
        if not hmac.compare_digest(returned_etag, commit_spec["etag"]):
            raise RuntimeError(f"PUT {commit_key} ETag mismatch")
        snapshot = metadata_verifier.list_generation_versions(
            remote_prefix, allowed_keys
        )
        _validate_pinned_snapshot_objects(
            metadata_verifier, snapshot, file_specs, file_pins
        )
        _snapshot_object_version(
            metadata_verifier,
            snapshot,
            manifest_key,
            manifest_spec,
            manifest_version_id,
        )
        _snapshot_object_version(
            metadata_verifier,
            snapshot,
            commit_key,
            commit_spec,
            commit_version_id,
        )

    return {
        "commit_key": commit_key,
        "commit_sha256": commit_spec["sha256"],
        "commit_version_id": commit_version_id,
        "file_version_ids": file_version_ids,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_spec["sha256"],
        "manifest_version_id": manifest_version_id,
    }


def upload_generation(
    client: S3Client,
    generation_dir: Path,
    generation_id: str,
    remote_prefix: str,
    receipt_path: Path,
    predecessor_manifest_sha256: str | None = None,
    metadata_verifier: B2NativeObjectVerifier | None = None,
):
    generation_dir = Path(generation_dir).absolute()
    generation_id = validate_generation_id(generation_id)
    remote_prefix = normalize_remote_prefix(remote_prefix)
    receipt_candidate = Path(receipt_path).absolute()
    try:
        receipt_common = Path(os.path.commonpath((generation_dir, receipt_candidate)))
    except ValueError:
        receipt_common = None
    if receipt_common == generation_dir:
        raise ValueError("generation receipt must be outside the generation directory")
    with lock_stopped_generation(generation_dir):
        local_files, total_bytes = build_generation_file_records(
            generation_dir,
            remote_prefix,
        )
        oversized_files = [
            record["path"]
            for record in local_files
            if record["size"] > IMMUTABLE_GENERATION_SINGLE_PUT_LIMIT
        ]
        if oversized_files:
            raise ValueError(
                "immutable generation object exceeds the single-PUT limit: "
                + ", ".join(oversized_files)
            )
        file_by_relative = dict(walk_generation(generation_dir))
        if metadata_verifier is not None:
            publication = _upload_generation_with_native_snapshot_verification(
                client,
                generation_id,
                remote_prefix,
                local_files,
                total_bytes,
                file_by_relative,
                predecessor_manifest_sha256,
                metadata_verifier,
            )
            file_version_ids = publication["file_version_ids"]
        else:
            file_version_ids = {}
            for record in local_files:
                uploaded = upload_verified_file(
                    client,
                    file_by_relative[record["path"]],
                    record["object_key"],
                    multipart_threshold=IMMUTABLE_GENERATION_SINGLE_PUT_LIMIT + 1,
                    expected_size=record["size"],
                    expected_sha256=record["sha256"],
                    full_readback=False,
                )
                file_version_ids[record["path"]] = uploaded["version_id"]

        final_files, final_total_bytes = build_generation_file_records(
            generation_dir,
            remote_prefix,
        )
        if final_files != local_files or final_total_bytes != total_bytes:
            raise RuntimeError("generation changed while it was being uploaded")

        if metadata_verifier is not None:
            manifest_key = publication["manifest_key"]
            manifest_sha256 = publication["manifest_sha256"]
            manifest_version_id = publication["manifest_version_id"]
            commit_key = publication["commit_key"]
            commit_sha256 = publication["commit_sha256"]
            commit_version_id = publication["commit_version_id"]
        else:
            manifest = build_generation_manifest(
                generation_id,
                local_files,
                file_version_ids,
                total_bytes,
                predecessor_manifest_sha256,
            )
            manifest_bytes = canonical_json_bytes(manifest)
            manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
            manifest_key = f"{remote_prefix}/manifest.json"
            uploaded_manifest = upload_verified_bytes(
                client,
                manifest_bytes,
                manifest_key,
                full_readback=False,
            )
            manifest_version_id = uploaded_manifest["version_id"]

            commit_key = f"{remote_prefix}/_COMMITTED"
            commit_bytes = generation_commit_payload(
                generation_id,
                manifest_key,
                manifest_sha256,
                manifest_version_id,
                len(local_files),
                total_bytes,
                predecessor_manifest_sha256,
            )
            commit_sha256 = hashlib.sha256(commit_bytes).hexdigest()
            uploaded_commit = upload_verified_bytes(
                client,
                commit_bytes,
                commit_key,
                full_readback=False,
            )
            commit_version_id = uploaded_commit["version_id"]

        receipt = {
            "commit_key": commit_key,
            "commit_sha256": commit_sha256,
            "commit_version_id": commit_version_id,
            "file_count": len(local_files),
            "generation_id": generation_id,
            "manifest_key": manifest_key,
            "manifest_sha256": manifest_sha256,
            "manifest_version_id": manifest_version_id,
            "remote_prefix": remote_prefix,
            "schema_version": GENERATION_RECEIPT_SCHEMA_VERSION,
            "total_bytes": total_bytes,
            "verified_unix_secs": int(time.time()),
        }
        if predecessor_manifest_sha256:
            receipt["predecessor_manifest_sha256"] = validate_sha256(
                predecessor_manifest_sha256,
                "predecessor manifest SHA-256",
            )
        write_receipt_atomic(receipt_path, receipt, generation_dir)
        return receipt


def add_storage_arguments(parser):
    parser.add_argument(
        "--credentials-file",
        type=Path,
        help="literal dotenv file containing storage settings; never sourced as shell code",
    )
    parser.add_argument("--retries", type=int, default=8)


def argument_parser():
    parser = argparse.ArgumentParser(
        description="Upload and independently verify immutable S3-compatible objects."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    upload_file = commands.add_parser(
        "upload-file", help="upload, HEAD, and full-GET one file"
    )
    upload_file.add_argument("path", type=Path)
    upload_file.add_argument("key")
    upload_file.add_argument("--content-type", default="application/octet-stream")
    upload_file.add_argument("--part-size", type=int, default=DEFAULT_PART_SIZE)
    add_storage_arguments(upload_file)

    commit = commands.add_parser(
        "commit-marker",
        help="publish and fully verify an immutable generation commit marker",
    )
    commit.add_argument("key")
    commit.add_argument("--generation-id", required=True)
    commit.add_argument("--manifest-key", required=True)
    commit.add_argument("--manifest-sha256", required=True)
    commit.add_argument("--manifest-version-id", required=True)
    commit.add_argument("--predecessor-manifest-sha256")
    commit.add_argument("--file-count", required=True, type=int)
    commit.add_argument("--total-bytes", required=True, type=int)
    add_storage_arguments(commit)

    generation = commands.add_parser(
        "upload-generation",
        help="manifest and commit a stopped self-contained WAL generation without deleting it",
    )
    generation.add_argument("generation_dir", type=Path)
    generation.add_argument("remote_prefix")
    generation.add_argument("receipt", type=Path)
    generation.add_argument("--generation-id", required=True)
    generation.add_argument("--predecessor-manifest-sha256")
    add_storage_arguments(generation)

    account_usage = commands.add_parser(
        "b2-account-usage",
        help=(
            "read every Backblaze bucket, object version, and unfinished upload "
            "and report account-wide stored bytes"
        ),
    )
    add_storage_arguments(account_usage)
    return parser


def make_client(args):
    endpoint, region, bucket, access_key, secret_key = storage_settings(
        args.credentials_file
    )
    return S3Client(endpoint, region, bucket, access_key, secret_key, args.retries)


def make_b2_native_client(args):
    application_key_id, application_key = backblaze_native_settings(
        args.credentials_file
    )
    return B2NativeClient(
        application_key_id,
        application_key,
        args.retries,
        authorize_url=B2_AUTHORIZE_ACCOUNT_URL,
    )


def make_optional_b2_native_object_verifier(args, s3_client: S3Client):
    settings = optional_backblaze_native_object_settings(args.credentials_file)
    if settings is None:
        endpoint_host = urllib.parse.urlparse(s3_client.endpoint).hostname or ""
        if endpoint_host == "backblazeb2.com" or endpoint_host.endswith(
            ".backblazeb2.com"
        ):
            raise ValueError(
                "B2_BUCKET_ID is required for cap-safe Backblaze generation verification"
            )
        return None
    application_key_id, application_key, bucket_id = settings
    return B2NativeObjectVerifier(
        B2NativeClient(
            application_key_id,
            application_key,
            args.retries,
            authorize_url=B2_AUTHORIZE_ACCOUNT_URL,
        ),
        bucket_id,
        s3_client.bucket,
    )


def run(args):
    if args.command == "b2-account-usage":
        result = b2_account_usage(make_b2_native_client(args))
        print(canonical_json_bytes(result).decode("utf-8"), end="")
        return 0

    client = make_client(args)
    if args.command == "upload-file":
        result = upload_verified_file(
            client,
            args.path,
            args.key,
            args.content_type,
            args.part_size,
        )
        print(canonical_json_bytes(result).decode("utf-8"), end="")
        return 0
    if args.command == "commit-marker":
        if not args.key.endswith("/_COMMITTED"):
            raise ValueError("commit marker key must end with /_COMMITTED")
        expected_manifest_key = f"{args.key[: -len('/_COMMITTED')]}/manifest.json"
        if args.manifest_key != expected_manifest_key:
            raise ValueError(
                "manifest key must share the commit marker's immutable prefix"
            )
        verify_remote_sha256(
            client,
            args.manifest_key,
            args.manifest_sha256,
            args.manifest_version_id,
        )
        payload = generation_commit_payload(
            args.generation_id,
            args.manifest_key,
            args.manifest_sha256,
            args.manifest_version_id,
            args.file_count,
            args.total_bytes,
            args.predecessor_manifest_sha256,
        )
        result = upload_verified_bytes(client, payload, args.key)
        print(canonical_json_bytes(result).decode("utf-8"), end="")
        return 0
    if args.command == "upload-generation":
        metadata_verifier = make_optional_b2_native_object_verifier(args, client)
        receipt = upload_generation(
            client,
            args.generation_dir,
            args.generation_id,
            args.remote_prefix,
            args.receipt,
            args.predecessor_manifest_sha256,
            metadata_verifier,
        )
        print(canonical_json_bytes(receipt).decode("utf-8"), end="")
        return 0
    raise RuntimeError(f"unsupported command {args.command}")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    # Retain the historical `PATH KEY [options]` invocation for existing callers.
    if argv and argv[0] not in {
        "upload-file",
        "commit-marker",
        "upload-generation",
        "b2-account-usage",
        "-h",
        "--help",
    }:
        argv.insert(0, "upload-file")
    parser = argument_parser()
    try:
        return run(parser.parse_args(argv))
    except (OSError, ValueError, RuntimeError, requests.RequestException) as error:
        print(f"upload failed: {error}", file=sys.stderr)
        return backblaze_capacity_exit_status(error)


if __name__ == "__main__":
    raise SystemExit(main())
