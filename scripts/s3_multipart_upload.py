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
GENERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
GENERATION_MANIFEST_SCHEMA_VERSION = 1
GENERATION_COMMIT_SCHEMA_VERSION = 1
GENERATION_RECEIPT_SCHEMA_VERSION = 1
READ_CHUNK_SIZE = 1024 * 1024
MAX_CREDENTIALS_FILE_BYTES = 64 * 1024
MAX_VERSION_ID_BYTES = 1024
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


def signing_key(secret: str, date: str, region: str) -> bytes:
    key = hmac.new(("AWS4" + secret).encode(), date.encode(), hashlib.sha256).digest()
    key = hmac.new(key, region.encode(), hashlib.sha256).digest()
    key = hmac.new(key, b"s3", hashlib.sha256).digest()
    return hmac.new(key, b"aws4_request", hashlib.sha256).digest()


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
                    return response
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    response.close()
                    raise RuntimeError(
                        f"{method} {key} failed HTTP {response.status_code}"
                    )
                last_error = RuntimeError(
                    f"{method} {key} failed HTTP {response.status_code}"
                )
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
            stream=stream,
        )


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
    response = client.request(
        "PUT", key, headers=object_headers(content_type, sha256), body=data
    )
    try:
        return response_version_id(response, "PUT", key)
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
    response = client.request(
        "PUT", key, headers=object_headers(content_type, sha256), body=data
    )
    try:
        return response_version_id(response, "PUT", key)
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


def sha256_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(READ_CHUNK_SIZE)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _head_matches(
    response,
    expected_size: int,
    expected_sha256: str,
    key: str,
    expected_version_id: str | None = None,
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
    return version_id


def verify_remote_object(
    client: S3Client,
    key: str,
    expected_size: int,
    expected_sha256: str,
    version_id: str,
):
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    version_id = validate_version_id(version_id)
    version_params = {"versionId": version_id}
    head = client.request("HEAD", key, params=version_params)
    try:
        _head_matches(head, expected_size, expected_sha256, key, version_id)
    finally:
        head.close()

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
) -> str | None:
    expected_sha256 = validate_sha256(expected_sha256, "expected SHA-256")
    response = client.request("HEAD", key, allowed_statuses=(404,))
    try:
        if response.status_code == 404:
            return None
        version_id = _head_matches(response, expected_size, expected_sha256, key)
    finally:
        response.close()
    verify_remote_object(client, key, expected_size, expected_sha256, version_id)
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
):
    path = Path(path)
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"upload source must be a regular file: {path}")
    measured_size, measured_sha256 = sha256_file(path)
    if expected_size is not None and measured_size != expected_size:
        raise RuntimeError(f"local file size changed before upload: {path}")
    if expected_sha256 is not None and not hmac.compare_digest(
        measured_sha256, validate_sha256(expected_sha256, "expected local SHA-256")
    ):
        raise RuntimeError(f"local file SHA-256 changed before upload: {path}")
    size = measured_size
    sha256 = measured_sha256

    existing_version_id = latest_exact_object_version(client, key, size, sha256)
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
        version_id = single_put(client, path, key, content_type, sha256)
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
    verify_remote_object(client, key, size, sha256, version_id)
    if latest_exact_object_version(client, key, size, sha256) is None:
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
):
    sha256 = hashlib.sha256(data).hexdigest()
    size = len(data)
    existing_version_id = latest_exact_object_version(client, key, size, sha256)
    if existing_version_id is not None:
        return {
            "key": key,
            "size": size,
            "sha256": sha256,
            "version_id": existing_version_id,
            "already_present": True,
        }
    version_id = single_put_bytes(client, data, key, content_type, sha256)
    verify_remote_object(client, key, size, sha256, version_id)
    if latest_exact_object_version(client, key, size, sha256) is None:
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


def upload_generation(
    client: S3Client,
    generation_dir: Path,
    generation_id: str,
    remote_prefix: str,
    receipt_path: Path,
    predecessor_manifest_sha256: str | None = None,
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
        file_version_ids = {}
        for record in local_files:
            uploaded = upload_verified_file(
                client,
                file_by_relative[record["path"]],
                record["object_key"],
                multipart_threshold=IMMUTABLE_GENERATION_SINGLE_PUT_LIMIT + 1,
                expected_size=record["size"],
                expected_sha256=record["sha256"],
            )
            file_version_ids[record["path"]] = uploaded["version_id"]

        final_files, final_total_bytes = build_generation_file_records(
            generation_dir,
            remote_prefix,
        )
        if final_files != local_files or final_total_bytes != total_bytes:
            raise RuntimeError("generation changed while it was being uploaded")

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
        uploaded_manifest = upload_verified_bytes(client, manifest_bytes, manifest_key)
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
        uploaded_commit = upload_verified_bytes(client, commit_bytes, commit_key)
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
    return parser


def make_client(args):
    endpoint, region, bucket, access_key, secret_key = storage_settings(
        args.credentials_file
    )
    return S3Client(endpoint, region, bucket, access_key, secret_key, args.retries)


def run(args):
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
        receipt = upload_generation(
            client,
            args.generation_dir,
            args.generation_id,
            args.remote_prefix,
            args.receipt,
            args.predecessor_manifest_sha256,
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
        "-h",
        "--help",
    }:
        argv.insert(0, "upload-file")
    parser = argument_parser()
    try:
        return run(parser.parse_args(argv))
    except (OSError, ValueError, RuntimeError, requests.RequestException) as error:
        print(f"upload failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
