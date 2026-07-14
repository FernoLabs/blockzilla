#!/usr/bin/env python3
import contextlib
import fcntl
import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.parse
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import s3_multipart_upload as uploader


class FakeS3State:
    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.next_upload_id = 1
        self.next_version_id = 1
        self.requests = []
        self.corrupt_get_keys = set()
        self.corrupt_etag_keys = set()
        self.inject_after_put = {}
        self.lock = threading.Lock()

    def add_version(self, key, data, sha256):
        version_id = f"version-{self.next_version_id:08d}"
        self.next_version_id += 1
        stored = {
            "data": data,
            "etag": hashlib.md5(data, usedforsecurity=False).hexdigest(),
            "sha256": sha256,
            "version_id": version_id,
        }
        self.objects.setdefault(key, []).append(stored)
        return stored

    def object_version(self, key, version_id=None):
        versions = self.objects.get(key, [])
        if version_id is None:
            return versions[-1] if versions else None
        return next(
            (stored for stored in versions if stored["version_id"] == version_id),
            None,
        )


class FakeS3Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state = None

    def log_message(self, _format, *_args):
        return

    def object_key(self):
        parsed = urllib.parse.urlparse(self.path)
        components = parsed.path.split("/", 2)
        if len(components) != 3 or not components[1]:
            self.send_empty(400)
            return None, None
        return urllib.parse.unquote(components[2]), urllib.parse.parse_qs(
            parsed.query,
            keep_blank_values=True,
        )

    def record(self, method, key, query):
        headers = {name.lower(): value for name, value in self.headers.items()}
        with self.state.lock:
            self.state.requests.append((method, key, headers, dict(query)))

    def read_body(self):
        return self.rfile.read(int(self.headers.get("content-length", "0")))

    def send_empty(self, status, headers=None):
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, str(value))
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_bytes(self, status, body, headers=None, *, include_body=True):
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def do_HEAD(self):
        key, query = self.object_key()
        if key is None:
            return
        self.record("HEAD", key, query)
        requested_version = query.get("versionId", [None])[0]
        with self.state.lock:
            stored = self.state.object_version(key, requested_version)
            corrupt_etag = key in self.state.corrupt_etag_keys
        if stored is None:
            self.send_empty(404)
            return
        self.send_bytes(
            200,
            stored["data"],
            {
                "x-amz-meta-sha256": stored["sha256"],
                "x-amz-version-id": stored["version_id"],
                "ETag": '"00000000000000000000000000000000"'
                if corrupt_etag
                else f'"{stored["etag"]}"',
            },
            include_body=False,
        )

    def do_GET(self):
        key, query = self.object_key()
        if key is None:
            return
        self.record("GET", key, query)
        requested_version = query.get("versionId", [None])[0]
        with self.state.lock:
            stored = self.state.object_version(key, requested_version)
            corrupt = key in self.state.corrupt_get_keys
            corrupt_etag = key in self.state.corrupt_etag_keys
        if stored is None:
            self.send_empty(404)
            return
        body = stored["data"]
        if corrupt and body:
            body = bytes([body[0] ^ 1]) + body[1:]
        self.send_bytes(
            200,
            body,
            {
                "x-amz-meta-sha256": stored["sha256"],
                "x-amz-version-id": stored["version_id"],
                "ETag": '"00000000000000000000000000000000"'
                if corrupt_etag
                else f'"{stored["etag"]}"',
            },
        )

    def do_PUT(self):
        key, query = self.object_key()
        if key is None:
            return
        self.record("PUT", key, query)
        body = self.read_body()
        upload_ids = query.get("uploadId")
        part_numbers = query.get("partNumber")
        if upload_ids and part_numbers:
            upload_id = upload_ids[0]
            with self.state.lock:
                upload = self.state.uploads.get(upload_id)
                if upload is None or upload["key"] != key:
                    self.send_empty(404)
                    return
                upload["parts"][int(part_numbers[0])] = body
            etag = hashlib.md5(body, usedforsecurity=False).hexdigest()
            self.send_empty(200, {"ETag": f'"{etag}"'})
            return

        with self.state.lock:
            if self.headers.get("if-none-match") is not None:
                self.send_empty(501)
                return
            stored = self.state.add_version(
                key,
                body,
                self.headers.get("x-amz-meta-sha256", ""),
            )
            injected = self.state.inject_after_put.pop(key, None)
            if injected is not None:
                self.state.add_version(key, injected["data"], injected["sha256"])
            corrupt_etag = key in self.state.corrupt_etag_keys
        self.send_empty(
            200,
            {
                "ETag": '"00000000000000000000000000000000"'
                if corrupt_etag
                else f'"{stored["etag"]}"',
                "x-amz-version-id": stored["version_id"],
            },
        )

    def do_POST(self):
        key, query = self.object_key()
        if key is None:
            return
        self.record("POST", key, query)
        body = self.read_body()
        if "uploads" in query:
            with self.state.lock:
                upload_id = f"upload-{self.state.next_upload_id}"
                self.state.next_upload_id += 1
                self.state.uploads[upload_id] = {
                    "key": key,
                    "parts": {},
                    "sha256": self.headers.get("x-amz-meta-sha256", ""),
                }
            response = (
                "<InitiateMultipartUploadResult>"
                f"<UploadId>{upload_id}</UploadId>"
                "</InitiateMultipartUploadResult>"
            ).encode()
            self.send_bytes(200, response, {"Content-Type": "application/xml"})
            return
        upload_ids = query.get("uploadId")
        if not upload_ids:
            self.send_empty(400)
            return
        upload_id = upload_ids[0]
        with self.state.lock:
            upload = self.state.uploads.pop(upload_id, None)
            if upload is None or upload["key"] != key:
                self.send_empty(404)
                return
            data = b"".join(
                upload["parts"][number] for number in sorted(upload["parts"])
            )
            stored = self.state.add_version(key, data, upload["sha256"])
        self.send_bytes(
            200,
            b"<CompleteMultipartUploadResult/>",
            {
                "ETag": f'"{stored["etag"]}-1"',
                "x-amz-version-id": stored["version_id"],
            },
        )

    def do_DELETE(self):
        key, query = self.object_key()
        if key is None:
            return
        self.record("DELETE", key, query)
        upload_ids = query.get("uploadId")
        if upload_ids:
            with self.state.lock:
                self.state.uploads.pop(upload_ids[0], None)
        self.send_empty(204)


class FakeB2State:
    def __init__(self):
        self.api_url = None
        self.allowed = {
            "bucketId": None,
            "capabilities": ["listBuckets", "listFiles", "writeFiles"],
            "namePrefix": None,
        }
        self.authorization_statuses = []
        self.requests = []
        self.routes = {}
        self.lock = threading.Lock()

    def enqueue(self, method, operation, payload, status=200, headers=None):
        path = f"/b2api/v4/{operation}"
        self.routes.setdefault((method, path), []).append(
            (status, payload, headers or {})
        )


class FakeB2Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state = None

    def log_message(self, _format, *_args):
        return

    def send_json(self, status, payload, headers=None):
        body = json.dumps(payload, sort_keys=True).encode()
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, str(value))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_request(self, method):
        parsed = urllib.parse.urlparse(self.path)
        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        try:
            json_body = json.loads(body) if body else None
        except json.JSONDecodeError:
            self.send_json(400, {"code": "bad_json"})
            return
        headers = {name.lower(): value for name, value in self.headers.items()}
        with self.state.lock:
            self.state.requests.append(
                {
                    "body": json_body,
                    "headers": headers,
                    "method": method,
                    "path": parsed.path,
                    "query": query,
                }
            )

        if parsed.path.endswith("/b2_authorize_account"):
            with self.state.lock:
                status = (
                    self.state.authorization_statuses.pop(0)
                    if self.state.authorization_statuses
                    else 200
                )
            if status != 200:
                self.send_json(
                    status,
                    {"code": "service_unavailable", "status": status},
                )
                return
            self.send_json(
                200,
                {
                    "accountId": "fake-account",
                    "authorizationToken": "fake-native-token",
                    "apiInfo": {
                        "storageApi": {
                            "allowed": self.state.allowed,
                            "apiUrl": self.state.api_url,
                        }
                    },
                },
            )
            return

        if headers.get("authorization") != "fake-native-token":
            self.send_json(401, {"code": "bad_auth_token", "status": 401})
            return
        route = (method, parsed.path)
        with self.state.lock:
            queued = self.state.routes.get(route, [])
            response = queued.pop(0) if queued else None
        if response is None:
            self.send_json(500, {"code": "unexpected_test_request", "status": 500})
            return
        status, payload, response_headers = response
        self.send_json(status, payload, response_headers)

    def do_GET(self):
        self.handle_request("GET")

    def do_POST(self):
        self.handle_request("POST")


class S3UploaderTests(unittest.TestCase):
    def setUp(self):
        self.state = FakeS3State()
        handler = type("BoundFakeS3Handler", (FakeS3Handler,), {"state": self.state})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        endpoint = f"http://127.0.0.1:{self.server.server_port}"
        self.client = uploader.S3Client(
            endpoint, "test-region", "test-bucket", "fake-id", "fake-key", 0
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary.cleanup()

    def create_generation(self):
        generation = self.root / "generation"
        writer_lock = generation / "wal" / "journal" / "writer.lock"
        writer_lock.parent.mkdir(parents=True)
        writer_lock.write_bytes(b"")
        (writer_lock.parent / "segment-00000000000000000000.wal").write_bytes(
            b"durable wal"
        )
        (generation / "identity.json").write_text('{"schema_version":1}\n')
        (generation / "raw-blocks.jsonl").write_text('{"frame_id":0,"slot":1}\n')
        return generation, writer_lock

    def create_credentials(self):
        credentials = self.root / "credentials.env"
        credentials.write_text(
            "\n".join(
                [
                    f"B2_S3_ENDPOINT=http://127.0.0.1:{self.server.server_port}",
                    "B2_S3_REGION=test-region",
                    "B2_BUCKET=test-bucket",
                    "AWS_ACCESS_KEY_ID=fake-id",
                    "AWS_SECRET_ACCESS_KEY=fake-key",
                    "",
                ]
            )
        )
        return credentials

    def test_credentials_file_is_literal_and_never_executes_shell(self):
        marker = self.root / "must-not-exist"
        credentials = self.root / "credentials.env"
        credentials.write_text(
            "\n".join(
                [
                    "B2_S3_ENDPOINT=https://s3.example.invalid",
                    "B2_S3_REGION=test-region",
                    "B2_BUCKET=test-bucket",
                    "AWS_ACCESS_KEY_ID=fake-id",
                    f"AWS_SECRET_ACCESS_KEY=$(touch${{IFS}}{marker})",
                    "",
                ]
            )
        )
        settings = uploader.storage_settings(credentials)
        self.assertEqual(settings[2], "test-bucket")
        self.assertTrue(settings[4].startswith("$(touch"))
        self.assertFalse(marker.exists())

        credentials.write_text(
            credentials.read_text() + "AWS_ACCESS_KEY_ID=duplicate\n"
        )
        with self.assertRaisesRegex(ValueError, "duplicate key"):
            uploader.parse_credentials_file(credentials)

    def test_upload_file_uses_metadata_head_and_streamed_get(self):
        path = self.root / "segment.wal"
        path.write_bytes(b"verified payload")
        result = uploader.upload_verified_file(
            self.client, path, "prefix/files/segment.wal"
        )
        self.assertFalse(result["already_present"])
        stored = self.state.object_version(
            "prefix/files/segment.wal", result["version_id"]
        )
        self.assertEqual(stored["data"], path.read_bytes())
        self.assertEqual(
            stored["sha256"], hashlib.sha256(path.read_bytes()).hexdigest()
        )
        methods = [
            method
            for method, key, _headers, _query in self.state.requests
            if key.endswith("segment.wal")
        ]
        self.assertEqual(
            methods,
            ["HEAD", "PUT", "HEAD", "GET", "HEAD", "HEAD", "GET"],
        )
        put_headers = next(
            headers
            for method, key, headers, _query in self.state.requests
            if method == "PUT" and key.endswith("segment.wal")
        )
        self.assertNotIn("if-none-match", put_headers)
        versioned_reads = [
            query["versionId"][0]
            for method, key, _headers, query in self.state.requests
            if method in {"HEAD", "GET"}
            and key.endswith("segment.wal")
            and "versionId" in query
        ]
        self.assertEqual(versioned_reads, [result["version_id"]] * 4)
        for _method, _key, headers, _query in self.state.requests:
            self.assertIn("authorization", headers)
            self.assertNotIn("fake-key", json.dumps(headers))

    def test_full_get_corruption_fails_verification(self):
        path = self.root / "corrupt.wal"
        path.write_bytes(b"payload")
        key = "prefix/files/corrupt.wal"
        self.state.corrupt_get_keys.add(key)
        with self.assertRaisesRegex(RuntimeError, "GET .* SHA-256 mismatch"):
            uploader.upload_verified_file(self.client, path, key)

    def test_multipart_upload_preserves_whole_object_digest(self):
        path = self.root / "multipart.wal"
        path.write_bytes(b"abcdefghijklmnopqrstuvwxyz")
        result = uploader.upload_verified_file(
            self.client,
            path,
            "prefix/files/multipart.wal",
            part_size=5,
            multipart_threshold=1,
        )
        self.assertFalse(result["already_present"])
        stored = self.state.object_version(
            "prefix/files/multipart.wal", result["version_id"]
        )
        self.assertEqual(stored["data"], path.read_bytes())
        self.assertEqual(
            stored["sha256"], hashlib.sha256(path.read_bytes()).hexdigest()
        )

    def test_generation_upload_commits_last_and_writes_durable_receipt(self):
        generation, _writer_lock = self.create_generation()
        receipt_path = self.root / "receipts" / "generation-1.json"
        predecessor = "ab" * 32
        receipt = uploader.upload_generation(
            self.client,
            generation,
            "generation-1",
            "grpc-raw/v1/generation-1/",
            receipt_path,
            predecessor,
        )
        self.assertEqual(receipt["schema_version"], 1)
        self.assertEqual(receipt["remote_prefix"], "grpc-raw/v1/generation-1")
        self.assertTrue(receipt_path.is_file())
        self.assertEqual(receipt, json.loads(receipt_path.read_text()))
        self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o600)
        self.assertTrue(
            generation.is_dir(), "uploader must never delete the generation"
        )

        put_keys = [
            key
            for method, key, _headers, _query in self.state.requests
            if method == "PUT"
        ]
        self.assertEqual(
            put_keys[-2:], [receipt["manifest_key"], receipt["commit_key"]]
        )
        manifest = json.loads(
            self.state.object_version(
                receipt["manifest_key"], receipt["manifest_version_id"]
            )["data"]
        )
        commit = json.loads(
            self.state.object_version(
                receipt["commit_key"], receipt["commit_version_id"]
            )["data"]
        )
        self.assertEqual(manifest["predecessor_manifest_sha256"], predecessor)
        self.assertTrue(all(record["version_id"] for record in manifest["files"]))
        self.assertEqual(commit["manifest_sha256"], receipt["manifest_sha256"])
        self.assertEqual(commit["manifest_version_id"], receipt["manifest_version_id"])
        self.assertEqual(commit["file_count"], receipt["file_count"])
        self.assertTrue(receipt["commit_version_id"])

        before_puts = len(put_keys)
        uploader.upload_generation(
            self.client,
            generation,
            "generation-1",
            "grpc-raw/v1/generation-1",
            receipt_path,
            predecessor,
        )
        after_puts = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        self.assertEqual(
            after_puts, before_puts, "exact committed retry must not overwrite"
        )

    def test_generation_upload_uses_only_single_put_objects(self):
        generation, _writer_lock = self.create_generation()
        uploader.upload_generation(
            self.client,
            generation,
            "generation-single-put",
            "grpc-raw/v1/generation-single-put",
            self.root / "receipt.json",
        )
        self.assertFalse(
            any(
                method == "POST"
                for method, _key, _headers, _query in self.state.requests
            )
        )
        self.assertFalse(
            any(
                method == "GET"
                for method, _key, _headers, _query in self.state.requests
            ),
            "routine generation publication must not consume B2 download bytes",
        )
        self.assertFalse(
            any(
                "if-none-match" in headers
                for method, _key, headers, _query in self.state.requests
                if method == "PUT"
            )
        )

    def test_generation_upload_rejects_a_wrong_single_put_etag(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-bad-etag"
        self.state.corrupt_etag_keys.add(f"{prefix}/files/identity.json")
        receipt_path = self.root / "bad-etag-receipt.json"
        with self.assertRaisesRegex(RuntimeError, "ETag mismatch"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-bad-etag",
                prefix,
                receipt_path,
            )
        self.assertFalse(receipt_path.exists())

    def test_generation_retry_rejects_a_wrong_remote_head_etag(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-bad-head-etag"
        receipt_path = self.root / "bad-head-etag-receipt.json"
        receipt = uploader.upload_generation(
            self.client,
            generation,
            "generation-bad-head-etag",
            prefix,
            receipt_path,
        )
        receipt_bytes = receipt_path.read_bytes()
        puts_before_retry = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        self.state.corrupt_etag_keys.add(f"{prefix}/files/identity.json")
        with self.assertRaisesRegex(RuntimeError, "HEAD .* ETag mismatch"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-bad-head-etag",
                prefix,
                receipt_path,
            )
        puts_after_retry = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        self.assertEqual(puts_after_retry, puts_before_retry)
        self.assertEqual(receipt_path.read_bytes(), receipt_bytes)
        self.assertEqual(json.loads(receipt_bytes), receipt)

    def test_upload_generation_cli_uses_credentials_file(self):
        generation, _writer_lock = self.create_generation()
        receipt_path = self.root / "receipts" / "generation-cli.json"
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = uploader.main(
                [
                    "upload-generation",
                    str(generation),
                    "grpc-raw/v1/generation-cli",
                    str(receipt_path),
                    "--generation-id",
                    "generation-cli",
                    "--credentials-file",
                    str(self.create_credentials()),
                    "--retries",
                    "0",
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            json.loads(output.getvalue()), json.loads(receipt_path.read_text())
        )

    def test_conflicting_commit_is_never_overwritten(self):
        generation, _writer_lock = self.create_generation()
        key = "grpc-raw/v1/generation-1/_COMMITTED"
        conflict = self.state.add_version(
            key, b"conflict", hashlib.sha256(b"conflict").hexdigest()
        )
        with self.assertRaisesRegex(RuntimeError, "HEAD .* mismatch"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-1",
                "grpc-raw/v1/generation-1",
                self.root / "receipt.json",
            )
        self.assertEqual(len(self.state.objects[key]), 1)
        self.assertEqual(self.state.object_version(key), conflict)
        self.assertFalse(
            any(
                method == "PUT" and request_key == key
                for method, request_key, _headers, _query in self.state.requests
            )
        )
        self.assertFalse((self.root / "receipt.json").exists())

    def test_conflicting_latest_file_fails_without_overwrite(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-1"
        files, _total_bytes = uploader.build_generation_file_records(generation, prefix)
        first = files[0]
        conflict = self.state.add_version(
            first["object_key"],
            b"conflict",
            hashlib.sha256(b"conflict").hexdigest(),
        )
        with self.assertRaisesRegex(RuntimeError, "HEAD .* mismatch"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-1",
                prefix,
                self.root / "receipt.json",
            )
        self.assertEqual(self.state.object_version(first["object_key"]), conflict)
        self.assertFalse(
            any(
                method == "PUT"
                for method, _key, _headers, _query in self.state.requests
            )
        )

    def test_concurrent_identical_put_and_later_overwrite_preserve_pinned_version(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-race"
        files, _total_bytes = uploader.build_generation_file_records(generation, prefix)
        raced_file = files[0]
        local_paths = dict(uploader.walk_generation(generation))
        concurrent_data = local_paths[raced_file["path"]].read_bytes()
        self.state.inject_after_put[raced_file["object_key"]] = {
            "data": concurrent_data,
            "sha256": raced_file["sha256"],
        }
        receipt_path = self.root / "race-receipt.json"
        receipt = uploader.upload_generation(
            self.client,
            generation,
            "generation-race",
            prefix,
            receipt_path,
        )
        manifest = json.loads(
            self.state.object_version(
                receipt["manifest_key"], receipt["manifest_version_id"]
            )["data"]
        )
        manifest_record = next(
            record
            for record in manifest["files"]
            if record["path"] == raced_file["path"]
        )
        pinned = self.state.object_version(
            raced_file["object_key"], manifest_record["version_id"]
        )
        latest = self.state.object_version(raced_file["object_key"])
        self.assertEqual(pinned["sha256"], raced_file["sha256"])
        self.assertEqual(latest["sha256"], raced_file["sha256"])
        self.assertNotEqual(pinned["version_id"], latest["version_id"])

        conflict_data = b"later conflicting latest version"
        self.state.add_version(
            raced_file["object_key"],
            conflict_data,
            hashlib.sha256(conflict_data).hexdigest(),
        )
        uploader.verify_remote_object(
            self.client,
            raced_file["object_key"],
            raced_file["size"],
            raced_file["sha256"],
            manifest_record["version_id"],
        )

        puts_before_retry = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        with self.assertRaisesRegex(RuntimeError, "HEAD .* mismatch"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-race",
                prefix,
                receipt_path,
            )
        puts_after_retry = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        self.assertEqual(puts_after_retry, puts_before_retry)
        self.assertEqual(json.loads(receipt_path.read_text()), receipt)

    def test_active_writer_symlink_and_nonregular_entries_are_rejected(self):
        generation, writer_lock = self.create_generation()
        descriptor = os.open(writer_lock, os.O_RDONLY)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with self.assertRaisesRegex(RuntimeError, "active writer"):
                uploader.upload_generation(
                    self.client,
                    generation,
                    "generation-1",
                    "grpc-raw/v1/generation-1",
                    self.root / "receipt.json",
                )
        finally:
            os.close(descriptor)

        symlink = generation / "unsafe-link"
        symlink.symlink_to(generation / "identity.json")
        with self.assertRaisesRegex(ValueError, "symlink"):
            uploader.walk_generation(generation)
        symlink.unlink()

        fifo = generation / "unsafe-fifo"
        os.mkfifo(fifo)
        with self.assertRaisesRegex(ValueError, "non-regular"):
            uploader.walk_generation(generation)

    def test_receipt_inside_generation_is_rejected_before_network_writes(self):
        generation, _writer_lock = self.create_generation()
        with self.assertRaisesRegex(ValueError, "outside"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-1",
                "grpc-raw/v1/generation-1",
                generation / "receipt.json",
            )
        self.assertEqual(self.state.requests, [])


class B2AccountUsageTests(unittest.TestCase):
    def setUp(self):
        self.state = FakeB2State()
        handler = type("BoundFakeB2Handler", (FakeB2Handler,), {"state": self.state})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"
        self.state.api_url = self.endpoint
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary.cleanup()

    def client(self, retries=0):
        return uploader.B2NativeClient(
            "fake-application-key-id",
            "fake-application-key-secret",
            retries,
            authorize_url=f"{self.endpoint}/b2api/v4/b2_authorize_account",
        )

    def enqueue_buckets(self, *bucket_ids):
        self.state.enqueue(
            "POST",
            "b2_list_buckets",
            {"buckets": [{"bucketId": bucket_id} for bucket_id in bucket_ids]},
        )

    def enqueue_no_unfinished(self):
        self.state.enqueue(
            "GET",
            "b2_list_unfinished_large_files",
            {"files": [], "nextFileId": None},
        )

    def test_account_usage_sums_every_version_and_paginates_all_buckets(self):
        self.enqueue_buckets("bucket-a", "bucket-b")
        self.enqueue_no_unfinished()
        self.enqueue_no_unfinished()
        self.state.enqueue(
            "GET",
            "b2_list_file_versions",
            {
                "files": [
                    {"action": "upload", "contentLength": 10},
                    {"action": "hide", "contentLength": 0},
                ],
                "nextFileId": "version-next",
                "nextFileName": "same-key",
            },
        )
        self.state.enqueue(
            "GET",
            "b2_list_file_versions",
            {
                "files": [
                    {"action": "upload", "contentLength": 20},
                    {"action": "folder", "contentLength": 0},
                ],
                "nextFileId": None,
                "nextFileName": None,
            },
        )
        self.state.enqueue(
            "GET",
            "b2_list_file_versions",
            {
                "files": [{"action": "upload", "contentLength": 30}],
                "nextFileId": None,
                "nextFileName": None,
            },
        )

        result = uploader.b2_account_usage(self.client())

        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["scope"], "account")
        self.assertIs(result["scope_complete"], True)
        self.assertEqual(result["bucket_count"], 2)
        self.assertEqual(result["upload_version_count"], 3)
        self.assertEqual(result["hide_marker_count"], 1)
        self.assertEqual(result["folder_entry_count"], 1)
        self.assertEqual(result["stored_upload_bytes"], 60)
        self.assertEqual(result["unfinished_part_bytes"], 0)
        self.assertEqual(result["total_stored_bytes"], 60)
        self.assertEqual(result["version_page_count"], 3)

        bucket_request = next(
            request
            for request in self.state.requests
            if request["path"].endswith("/b2_list_buckets")
        )
        self.assertEqual(bucket_request["body"]["bucketTypes"], ["all"])
        version_requests = [
            request
            for request in self.state.requests
            if request["path"].endswith("/b2_list_file_versions")
        ]
        self.assertEqual(version_requests[1]["query"]["startFileName"], ["same-key"])
        self.assertEqual(version_requests[1]["query"]["startFileId"], ["version-next"])

    def test_account_usage_includes_paginated_unfinished_parts(self):
        self.enqueue_buckets("bucket-a")
        self.state.enqueue(
            "GET",
            "b2_list_unfinished_large_files",
            {"files": [{"fileId": "unfinished-1"}], "nextFileId": "unfinished-2"},
        )
        self.state.enqueue(
            "GET",
            "b2_list_unfinished_large_files",
            {"files": [{"fileId": "unfinished-2"}], "nextFileId": None},
        )
        self.state.enqueue(
            "GET",
            "b2_list_parts",
            {
                "parts": [{"partNumber": 1, "contentLength": 100}],
                "nextPartNumber": 2,
            },
        )
        self.state.enqueue(
            "GET",
            "b2_list_parts",
            {
                "parts": [{"partNumber": 2, "contentLength": 200}],
                "nextPartNumber": None,
            },
        )
        self.state.enqueue(
            "GET",
            "b2_list_parts",
            {
                "parts": [{"partNumber": 1, "contentLength": 300}],
                "nextPartNumber": None,
            },
        )
        self.state.enqueue(
            "GET",
            "b2_list_file_versions",
            {
                "files": [{"action": "upload", "contentLength": 5}],
                "nextFileId": None,
                "nextFileName": None,
            },
        )

        result = uploader.b2_account_usage(self.client())

        self.assertEqual(result["unfinished_large_file_count"], 2)
        self.assertEqual(result["unfinished_large_file_page_count"], 2)
        self.assertEqual(result["unfinished_part_count"], 3)
        self.assertEqual(result["unfinished_part_page_count"], 3)
        self.assertEqual(result["unfinished_part_bytes"], 600)
        self.assertEqual(result["stored_upload_bytes"], 5)
        self.assertEqual(result["total_stored_bytes"], 605)
        unfinished_requests = [
            request
            for request in self.state.requests
            if request["path"].endswith("/b2_list_unfinished_large_files")
        ]
        self.assertEqual(
            unfinished_requests[1]["query"]["startFileId"], ["unfinished-2"]
        )
        part_requests = [
            request
            for request in self.state.requests
            if request["path"].endswith("/b2_list_parts")
        ]
        self.assertEqual(part_requests[1]["query"]["startPartNumber"], ["2"])
        self.assertEqual(part_requests[2]["query"]["fileId"], ["unfinished-2"])

    def test_repeated_or_malformed_pagination_fails_closed(self):
        self.enqueue_buckets("bucket-a")
        self.enqueue_no_unfinished()
        for _index in range(2):
            self.state.enqueue(
                "GET",
                "b2_list_file_versions",
                {
                    "files": [{"action": "upload", "contentLength": 1}],
                    "nextFileId": "repeated-id",
                    "nextFileName": "repeated-name",
                },
            )
        with self.assertRaisesRegex(RuntimeError, "pagination did not advance"):
            uploader.b2_account_usage(self.client())

    def test_retry_logs_are_sanitized_and_cli_output_is_canonical_json(self):
        self.state.authorization_statuses.append(503)
        self.enqueue_buckets()
        error_output = io.StringIO()
        with mock.patch.object(uploader.time, "sleep"), contextlib.redirect_stderr(
            error_output
        ):
            result = uploader.b2_account_usage(self.client(retries=1))
        self.assertEqual(result["total_stored_bytes"], 0)
        self.assertIn("retry 1/1", error_output.getvalue())
        self.assertNotIn("fake-application-key-secret", error_output.getvalue())
        self.assertNotIn("fake-native-token", error_output.getvalue())

        credentials = self.root / "backblaze.env"
        credentials.write_text(
            "B2_APPLICATION_KEY_ID=fake-application-key-id\n"
            "B2_APPLICATION_KEY=fake-application-key-secret\n"
        )
        self.enqueue_buckets()
        output = io.StringIO()
        with mock.patch.object(
            uploader,
            "B2_AUTHORIZE_ACCOUNT_URL",
            f"{self.endpoint}/b2api/v4/b2_authorize_account",
        ), contextlib.redirect_stdout(output):
            status = uploader.main(
                [
                    "b2-account-usage",
                    "--credentials-file",
                    str(credentials),
                    "--retries",
                    "0",
                ]
            )
        self.assertEqual(status, 0)
        decoded = json.loads(output.getvalue())
        self.assertEqual(decoded["total_stored_bytes"], 0)
        self.assertEqual(
            output.getvalue(), uploader.canonical_json_bytes(decoded).decode("utf-8")
        )

    def test_restricted_key_and_invalid_sizes_never_claim_complete_usage(self):
        self.state.allowed["bucketId"] = "bucket-a"
        with self.assertRaisesRegex(RuntimeError, "account usage is incomplete"):
            uploader.b2_account_usage(self.client())

        self.state.allowed["bucketId"] = None
        self.enqueue_buckets("bucket-a")
        self.enqueue_no_unfinished()
        self.state.enqueue(
            "GET",
            "b2_list_file_versions",
            {
                "files": [{"action": "upload", "contentLength": -1}],
                "nextFileId": None,
                "nextFileName": None,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "non-negative integer"):
            uploader.b2_account_usage(self.client())


if __name__ == "__main__":
    unittest.main()
