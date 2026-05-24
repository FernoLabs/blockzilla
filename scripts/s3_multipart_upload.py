#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import hmac
import os
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import requests


DEFAULT_PART_SIZE = 128 * 1024 * 1024
MULTIPART_THRESHOLD = 64 * 1024 * 1024
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def uri_encode(value: str) -> str:
    return urllib.parse.quote(value, safe="/-_.~")


def query_encode(params):
    return "&".join(
        f"{urllib.parse.quote(str(key), safe='-_.~')}={urllib.parse.quote(str(value), safe='-_.~')}"
        for key, value in sorted(params.items())
    )


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
        self.host = urllib.parse.urlparse(self.endpoint).netloc

    def url_path(self, key: str) -> str:
        return f"/{uri_encode(self.bucket)}/{uri_encode(key.lstrip('/'))}"

    def request(self, method: str, key: str, params=None, headers=None, body=b""):
        last_error = None
        for attempt in range(1, self.retries + 2):
            response = None
            try:
                response = self._request_once(method, key, params=params, headers=headers, body=body)
                if response.status_code < 300:
                    return response
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    raise RuntimeError(
                        f"{method} {key} failed HTTP {response.status_code}: "
                        f"{response.text[:500]}"
                    )
                last_error = RuntimeError(
                    f"{method} {key} failed HTTP {response.status_code}: {response.text[:500]}"
                )
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

    def _request_once(self, method: str, key: str, params=None, headers=None, body=b""):
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
            f"{name}:{str(value).strip()}\n" for name, value in sorted(signed_headers.items())
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
        return requests.request(method, url, headers=request_headers, data=body, timeout=300)


def xml_text(root, tag):
    for item in root.iter():
        if item.tag.endswith(tag):
            return item.text
    return None


def single_put(client: S3Client, path: Path, key: str, content_type: str):
    data = path.read_bytes()
    client.request("PUT", key, headers={"content-type": content_type}, body=data)


def multipart_put(client: S3Client, path: Path, key: str, content_type: str, part_size: int):
    upload_id = None
    parts = []
    try:
        response = client.request(
            "POST",
            key,
            params={"uploads": ""},
            headers={"content-type": content_type},
        )
        upload_id = xml_text(ET.fromstring(response.text), "UploadId")
        if not upload_id:
            raise RuntimeError(f"create multipart upload did not return UploadId for {key}")

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
        client.request("POST", key, params={"uploadId": upload_id}, body=body)
    except Exception:
        if upload_id:
            try:
                client.request("DELETE", key, params={"uploadId": upload_id})
            except Exception as abort_error:
                print(f"warning: failed to abort multipart upload: {abort_error}", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(description="Upload a file to S3-compatible storage.")
    parser.add_argument("path", type=Path)
    parser.add_argument("key")
    parser.add_argument("--content-type", default="application/octet-stream")
    parser.add_argument("--part-size", type=int, default=DEFAULT_PART_SIZE)
    parser.add_argument("--retries", type=int, default=8)
    args = parser.parse_args()

    endpoint = os.environ.get("B2_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("B2_S3_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    bucket = os.environ.get("B2_BUCKET")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("B2_APPLICATION_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("B2_APPLICATION_KEY")
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
        raise SystemExit(f"missing required env: {', '.join(missing)}")

    client = S3Client(endpoint, region, bucket, access_key, secret_key, args.retries)
    size = args.path.stat().st_size
    if size >= MULTIPART_THRESHOLD:
        multipart_put(client, args.path, args.key, args.content_type, args.part_size)
    else:
        single_put(client, args.path, args.key, args.content_type)
    print(f"uploaded s3://{bucket}/{args.key} bytes={size}")


if __name__ == "__main__":
    main()
