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
        self.head_status = None
        self.override_put_version_ids = {}
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
        if self.state.head_status is not None:
            self.send_empty(self.state.head_status)
            return
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
                "x-amz-version-id": self.state.override_put_version_ids.get(
                    key, stored["version_id"]
                ),
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


class FakeNativeVersionClient:
    def __init__(self, s3_state):
        self.s3_state = s3_state
        self.account_id = "fake-account"
        self.allowed = {
            "bucketId": "fake-bucket-id",
            "bucketName": "test-bucket",
            "capabilities": ["listFiles"],
            "namePrefix": None,
        }
        self.calls = []
        self.action_overrides = {}
        self.account_id_overrides = {}
        self.bucket_id_overrides = {}
        self.content_sha1_overrides = {}
        self.sha256_overrides = {}

    def authorize(self):
        return {}

    def require_file_version_list_access(self, bucket_id, key):
        if bucket_id != "fake-bucket-id":
            raise RuntimeError("unexpected fake bucket")
        prefix = self.allowed.get("namePrefix")
        if prefix is not None and not key.startswith(prefix):
            raise RuntimeError("key outside fake prefix")

    def _entry(self, key, stored):
        version_id = stored["version_id"]
        data = stored["data"]
        return {
            "accountId": self.account_id_overrides.get(
                version_id, self.account_id
            ),
            "action": self.action_overrides.get(version_id, "upload"),
            "bucketId": self.bucket_id_overrides.get(
                version_id, "fake-bucket-id"
            ),
            "contentLength": len(data),
            "contentMd5": stored["etag"],
            "contentSha1": self.content_sha1_overrides.get(
                version_id,
                hashlib.sha1(data, usedforsecurity=False).hexdigest(),
            ),
            "fileId": version_id,
            "fileInfo": {
                "sha256": self.sha256_overrides.get(
                    version_id, stored["sha256"]
                )
            },
            "fileName": key,
        }

    def api_request(self, operation, *, method="GET", params=None, json_body=None):
        if operation != "b2_list_file_versions" or method != "GET":
            raise RuntimeError(f"unexpected fake native operation {operation}")
        self.calls.append(dict(params or {}))
        start_name = params["startFileName"]
        start_id = params.get("startFileId")
        prefix = params.get("prefix", "")
        maximum = params["maxFileCount"]
        with self.s3_state.lock:
            ordered = [
                self._entry(key, stored)
                for key in sorted(self.s3_state.objects)
                if key >= start_name and key.startswith(prefix)
                for stored in reversed(self.s3_state.objects[key])
            ]
        if start_id is not None:
            exact_index = next(
                (
                    index
                    for index, entry in enumerate(ordered)
                    if entry["fileName"] == start_name
                    and entry["fileId"] == start_id
                ),
                None,
            )
            if exact_index is not None:
                ordered = ordered[exact_index:]
        page = ordered[:maximum]
        following = ordered[maximum] if len(ordered) > maximum else None
        return {
            "files": page,
            "nextFileId": following["fileId"] if following else None,
            "nextFileName": following["fileName"] if following else None,
        }


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

    def native_verifier(self):
        native_client = FakeNativeVersionClient(self.state)
        return native_client, uploader.B2NativeObjectVerifier(
            native_client, "fake-bucket-id", "test-bucket"
        )

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

    def test_native_generation_verification_works_while_s3_head_is_capped(self):
        generation, _writer_lock = self.create_generation()
        self.state.head_status = 403
        native_client, verifier = self.native_verifier()
        native_client.allowed["namePrefix"] = "grpc-raw/v1/generation-native"
        receipt = uploader.upload_generation(
            self.client,
            generation,
            "generation-native",
            "grpc-raw/v1/generation-native",
            self.root / "native-receipt.json",
            metadata_verifier=verifier,
        )
        self.assertTrue(receipt["commit_version_id"])
        self.assertFalse(
            any(
                method in {"HEAD", "GET"}
                for method, _key, _headers, _query in self.state.requests
            )
        )
        self.assertTrue(native_client.calls)
        self.assertTrue(
            all(
                call["prefix"] == "grpc-raw/v1/generation-native/"
                for call in native_client.calls
            )
        )
        exact_seeks = [call for call in native_client.calls if "startFileId" in call]
        self.assertFalse(exact_seeks)
        self.assertEqual(len(native_client.calls), 4)
        self.assertTrue(
            all(
                call["maxFileCount"] == uploader.B2_GENERATION_VERSION_PAGE_SIZE
                for call in native_client.calls
            )
        )

    def test_native_generation_verification_batches_502_files_and_retry(self):
        generation, _writer_lock = self.create_generation()
        chunks = generation / "chunks"
        chunks.mkdir()
        for index in range(498):
            (chunks / f"chunk-{index:04d}.bin").write_bytes(
                f"chunk {index}\n".encode()
            )
        self.assertEqual(len(uploader.walk_generation(generation)), 502)

        prefix = "grpc-raw/v1/generation-native-502"
        native_client, verifier = self.native_verifier()
        upload_counts = {"files": 0, "bytes": 0}

        def fake_single_put(_client, path, key, _content_type, sha256):
            data = path.read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), sha256)
            stored = self.state.add_version(key, data, sha256)
            upload_counts["files"] += 1
            return stored["version_id"], stored["etag"]

        def fake_single_put_bytes(_client, data, key, _content_type, sha256):
            self.assertEqual(hashlib.sha256(data).hexdigest(), sha256)
            stored = self.state.add_version(key, data, sha256)
            upload_counts["bytes"] += 1
            return stored["version_id"], stored["etag"]

        with mock.patch.object(
            uploader, "single_put", side_effect=fake_single_put
        ), mock.patch.object(
            uploader, "single_put_bytes", side_effect=fake_single_put_bytes
        ):
            receipt = uploader.upload_generation(
                self.client,
                generation,
                "generation-native-502",
                prefix,
                self.root / "native-502-receipt.json",
                metadata_verifier=verifier,
            )
            calls_after_publish = len(native_client.calls)
            puts_after_publish = dict(upload_counts)
            retried = uploader.upload_generation(
                self.client,
                generation,
                "generation-native-502",
                prefix,
                self.root / "native-502-receipt.json",
                metadata_verifier=verifier,
            )

        self.assertEqual(receipt["file_count"], 502)
        self.assertEqual(retried["commit_version_id"], receipt["commit_version_id"])
        self.assertEqual(calls_after_publish, 4)
        self.assertEqual(len(native_client.calls), 5)
        self.assertEqual(upload_counts, puts_after_publish)
        self.assertEqual(upload_counts, {"files": 502, "bytes": 2})
        self.assertTrue(
            all(
                call["prefix"] == f"{prefix}/"
                and call["maxFileCount"]
                == uploader.B2_GENERATION_VERSION_PAGE_SIZE
                for call in native_client.calls
            )
        )

    def test_native_generation_snapshot_rejects_unexpected_remote_key(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-native-unexpected"
        unexpected_key = f"{prefix}/files/not-local-anymore.wal"
        data = b"unexpected immutable object"
        self.state.add_version(
            unexpected_key, data, hashlib.sha256(data).hexdigest()
        )
        _native_client, verifier = self.native_verifier()
        with self.assertRaisesRegex(RuntimeError, "unexpected key"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-unexpected",
                prefix,
                self.root / "native-unexpected-receipt.json",
                metadata_verifier=verifier,
            )
        self.assertFalse(
            any(method == "PUT" for method, _key, _headers, _query in self.state.requests)
        )

    def test_native_generation_snapshot_paginates_by_prefix(self):
        prefix = "grpc-raw/v1/generation-native-pages"
        allowed_keys = set()
        expected_ids = set()
        for index in range(5):
            key = f"{prefix}/files/file-{index}.wal"
            data = f"file {index}\n".encode()
            stored = self.state.add_version(
                key, data, hashlib.sha256(data).hexdigest()
            )
            allowed_keys.add(key)
            expected_ids.add(stored["version_id"])
        native_client, verifier = self.native_verifier()

        with mock.patch.object(uploader, "B2_GENERATION_VERSION_PAGE_SIZE", 2):
            snapshot = verifier.list_generation_versions(prefix, allowed_keys)

        self.assertEqual(len(native_client.calls), 3)
        self.assertNotIn("startFileId", native_client.calls[0])
        self.assertIn("startFileId", native_client.calls[1])
        self.assertIn("startFileId", native_client.calls[2])
        returned_ids = {
            version["fileId"]
            for versions in snapshot.values()
            for version in versions
        }
        self.assertEqual(returned_ids, expected_ids)

    def test_native_manifest_version_is_pinned_before_commit_publication(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-native-manifest-pin"
        manifest_key = f"{prefix}/manifest.json"
        commit_key = f"{prefix}/_COMMITTED"
        self.state.override_put_version_ids[manifest_key] = "missing-version"
        _native_client, verifier = self.native_verifier()
        with self.assertRaisesRegex(RuntimeError, "different pinned version"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-manifest-pin",
                prefix,
                self.root / "native-manifest-pin-receipt.json",
                metadata_verifier=verifier,
            )
        self.assertFalse(
            any(
                method == "PUT" and key == commit_key
                for method, key, _headers, _query in self.state.requests
            )
        )

    def test_native_final_snapshot_blocks_receipt_on_commit_race(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-native-commit-race"
        commit_key = f"{prefix}/_COMMITTED"
        conflict = b"concurrent conflicting commit"
        self.state.inject_after_put[commit_key] = {
            "data": conflict,
            "sha256": hashlib.sha256(conflict).hexdigest(),
        }
        receipt_path = self.root / "native-commit-race-receipt.json"
        _native_client, verifier = self.native_verifier()

        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-commit-race",
                prefix,
                receipt_path,
                metadata_verifier=verifier,
            )

        self.assertFalse(receipt_path.exists())

    def test_native_retry_pins_existing_versions_without_new_puts(self):
        generation, _writer_lock = self.create_generation()
        self.state.head_status = 403
        _native_client, verifier = self.native_verifier()
        receipt_path = self.root / "native-retry-receipt.json"
        first = uploader.upload_generation(
            self.client,
            generation,
            "generation-native-retry",
            "grpc-raw/v1/generation-native-retry",
            receipt_path,
            metadata_verifier=verifier,
        )
        puts_before = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        second = uploader.upload_generation(
            self.client,
            generation,
            "generation-native-retry",
            "grpc-raw/v1/generation-native-retry",
            receipt_path,
            metadata_verifier=verifier,
        )
        puts_after = sum(
            method == "PUT" for method, _key, _headers, _query in self.state.requests
        )
        self.assertEqual(puts_after, puts_before)
        self.assertEqual(second["manifest_version_id"], first["manifest_version_id"])
        self.assertEqual(second["commit_version_id"], first["commit_version_id"])

    def test_native_verification_rejects_conflict_before_overwrite(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-native-conflict"
        files, _total_bytes = uploader.build_generation_file_records(generation, prefix)
        first_key = files[0]["object_key"]
        conflict = b"conflicting immutable payload"
        self.state.add_version(
            first_key, conflict, hashlib.sha256(conflict).hexdigest()
        )
        _native_client, verifier = self.native_verifier()
        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-conflict",
                prefix,
                self.root / "native-conflict-receipt.json",
                metadata_verifier=verifier,
            )
        self.assertFalse(
            any(method == "PUT" for method, _key, _headers, _query in self.state.requests)
        )

    def test_native_verification_rejects_wrong_put_version_and_raced_conflict(self):
        generation, _writer_lock = self.create_generation()
        prefix = "grpc-raw/v1/generation-native-wrong-version"
        files, _total_bytes = uploader.build_generation_file_records(generation, prefix)
        first = files[0]
        self.state.override_put_version_ids[first["object_key"]] = "missing-version"
        _native_client, verifier = self.native_verifier()
        with self.assertRaisesRegex(RuntimeError, "different pinned version"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-wrong-version",
                prefix,
                self.root / "native-wrong-version-receipt.json",
                metadata_verifier=verifier,
            )

        self.state.objects.clear()
        self.state.override_put_version_ids.clear()
        raced_data = b"raced conflicting payload"
        self.state.inject_after_put[first["object_key"]] = {
            "data": raced_data,
            "sha256": hashlib.sha256(raced_data).hexdigest(),
        }
        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            uploader.upload_generation(
                self.client,
                generation,
                "generation-native-wrong-version",
                prefix,
                self.root / "native-race-receipt.json",
                metadata_verifier=verifier,
            )

    def test_native_verification_rejects_identity_action_and_sha1_tampering(self):
        data = b"native metadata payload"
        key = "grpc-raw/v1/native-tamper"
        stored = self.state.add_version(key, data, hashlib.sha256(data).hexdigest())
        native_client, verifier = self.native_verifier()
        expected_sha1 = hashlib.sha1(data, usedforsecurity=False).hexdigest()
        native_client.account_id_overrides[stored["version_id"]] = "other-account"
        with self.assertRaisesRegex(RuntimeError, "different accountId"):
            verifier.latest_exact_version(
                key, len(data), stored["sha256"], expected_sha1, stored["etag"]
            )
        native_client.account_id_overrides.clear()
        native_client.bucket_id_overrides[stored["version_id"]] = "other-bucket"
        with self.assertRaisesRegex(RuntimeError, "different bucketId"):
            verifier.latest_exact_version(
                key, len(data), stored["sha256"], expected_sha1, stored["etag"]
            )
        native_client.bucket_id_overrides.clear()
        native_client.action_overrides[stored["version_id"]] = "hide"
        with self.assertRaisesRegex(RuntimeError, "unsupported action"):
            verifier.latest_exact_version(
                key, len(data), stored["sha256"], expected_sha1, stored["etag"]
            )
        native_client.action_overrides.clear()
        native_client.content_sha1_overrides[stored["version_id"]] = (
            "unverified:" + "0" * 40
        )
        with self.assertRaisesRegex(RuntimeError, "conflicting SHA-1"):
            verifier.latest_exact_version(
                key, len(data), stored["sha256"], expected_sha1, stored["etag"]
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


class BackblazeCapacityExitStatusTests(unittest.TestCase):
    @staticmethod
    def response(body, status=403):
        response = uploader.requests.Response()
        response.status_code = status
        response.headers["Content-Length"] = str(len(body))
        response._content = body
        response.raw = io.BytesIO(body)
        return response

    def test_bounded_error_parser_accepts_exact_native_json_and_s3_xml_codes(self):
        secret = "must-not-appear-in-errors"
        native = self.response(
            json.dumps(
                {
                    "code": "transaction_cap_exceeded",
                    "message": secret,
                    "status": 403,
                }
            ).encode()
        )
        s3 = self.response(
            (
                "<Error><Code>cap_exceeded</Code>"
                f"<Message>{secret}</Message></Error>"
            ).encode()
        )

        self.assertEqual(
            uploader.api_error_code(native), "transaction_cap_exceeded"
        )
        self.assertEqual(uploader.api_error_code(s3), "cap_exceeded")

        client = uploader.S3Client(
            "http://127.0.0.1:1",
            "test-region",
            "test-bucket",
            "test-access-key",
            "test-secret-key",
            0,
        )
        with mock.patch.object(client, "_request_once", return_value=s3):
            with self.assertRaises(uploader.S3APIError) as caught:
                client.request("PUT", "generation/data", body=b"data")
        self.assertEqual(caught.exception.code, "cap_exceeded")
        self.assertEqual(
            uploader.backblaze_capacity_exit_status(caught.exception), 22
        )
        self.assertNotIn(secret, str(caught.exception))

    def test_error_parser_rejects_oversized_malformed_and_ambiguous_bodies(self):
        oversized = self.response(
            json.dumps(
                {
                    "code": "transaction_cap_exceeded",
                    "message": "x" * uploader.MAX_API_ERROR_BODY_BYTES,
                }
            ).encode()
        )
        malformed = self.response(b'{"code":"transaction_cap_exceeded"')
        ambiguous = self.response(
            b"<Error><Code>cap_exceeded</Code>"
            b"<Code>transaction_cap_exceeded</Code></Error>"
        )

        self.assertEqual(uploader.api_error_code(oversized), "")
        self.assertEqual(uploader.api_error_code(malformed), "")
        self.assertEqual(uploader.api_error_code(ambiguous), "")

    def test_main_returns_stable_status_for_each_exact_capacity_code(self):
        cases = {
            "download_cap_exceeded": 20,
            "transaction_cap_exceeded": 21,
            "storage_cap_exceeded": 22,
            "cap_exceeded": 22,
        }
        for code, expected_status in cases.items():
            with self.subTest(code=code):
                error_output = io.StringIO()
                error = uploader.B2NativeAPIError("b2_test", 403, code)
                with mock.patch.object(
                    uploader, "run", side_effect=error
                ), contextlib.redirect_stderr(error_output):
                    status = uploader.main(["b2-account-usage"])
                self.assertEqual(status, expected_status)
                self.assertIn(code, error_output.getvalue())

    def test_unknown_or_untyped_403_is_generic_and_never_matched_by_text(self):
        cases = [
            uploader.B2NativeAPIError("b2_test", 403, ""),
            uploader.B2NativeAPIError(
                "b2_test", 403, "transaction_cap_exceeded_extra"
            ),
            uploader.B2NativeAPIError(
                "b2_test", 403, "TRANSACTION_CAP_EXCEEDED"
            ),
            RuntimeError("HTTP 403 transaction_cap_exceeded"),
        ]
        for error in cases:
            with self.subTest(error=repr(error)):
                error_output = io.StringIO()
                with mock.patch.object(
                    uploader, "run", side_effect=error
                ), contextlib.redirect_stderr(error_output):
                    status = uploader.main(["b2-account-usage"])
                self.assertEqual(status, 1)


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

    def test_native_transaction_cap_returns_21_without_retry_or_message_leak(self):
        secret = "server-message-must-stay-private"
        self.state.enqueue(
            "POST",
            "b2_list_buckets",
            {
                "code": "transaction_cap_exceeded",
                "message": secret,
                "status": 403,
            },
            status=403,
        )
        credentials = self.root / "backblaze-cap.env"
        credentials.write_text(
            "B2_APPLICATION_KEY_ID=fake-application-key-id\n"
            "B2_APPLICATION_KEY=fake-application-key-secret\n"
        )
        error_output = io.StringIO()
        with mock.patch.object(
            uploader,
            "B2_AUTHORIZE_ACCOUNT_URL",
            f"{self.endpoint}/b2api/v4/b2_authorize_account",
        ), contextlib.redirect_stderr(error_output):
            status = uploader.main(
                [
                    "b2-account-usage",
                    "--credentials-file",
                    str(credentials),
                    "--retries",
                    "8",
                ]
            )

        self.assertEqual(status, 21)
        self.assertEqual(len(self.state.requests), 2)
        self.assertIn("transaction_cap_exceeded", error_output.getvalue())
        self.assertNotIn(secret, error_output.getvalue())
        self.assertNotIn("fake-application-key-secret", error_output.getvalue())

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
