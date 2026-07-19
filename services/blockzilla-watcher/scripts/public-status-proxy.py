#!/usr/bin/env python3
"""Serve the watcher through a loopback proxy with public-safe telemetry."""

import argparse
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import re
import sys
import threading
from urllib.parse import urlsplit


MAX_JSON_BYTES = 64 * 1024 * 1024
COPY_REQUEST_HEADERS = {
    "accept",
    "cache-control",
    "if-modified-since",
    "if-none-match",
    "range",
    "user-agent",
}
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
PATH_KEY_SUFFIXES = ("_dir", "_path", "_root")
PRIVATE_LISTENER_NETWORKS = tuple(
    ipaddress.IPv4Network(cidr) for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
PUBLIC_API_GETS = {
    "/api/v1/events",
    "/api/v1/sidecars/block-time-gaps/status.json",
    "/api/v1/sidecars/runtime-operations/status.json",
    "/api/v1/status",
}
ABSOLUTE_PRIVATE_PATH = re.compile(
    r"(?<![A-Za-z0-9:])/(?:Applications|Users|bin|boot|data|dev|etc|home|lib|lib64|media|mnt|opt|proc|root|run|sbin|srv|storage|sys|tmp|usr|var|volume[0-9]*)"
    r"(?:/[^\s,;:()\[\]{}]+)*"
)


def safe_basename(value):
    if not isinstance(value, str):
        return value
    return value.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def sanitize_public_value(value, key=None):
    if isinstance(value, dict):
        return {
            child_key: sanitize_public_value(child_value, child_key)
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [sanitize_public_value(item) for item in value]
    if isinstance(value, str):
        if key == "path" or (isinstance(key, str) and key.endswith(PATH_KEY_SUFFIXES)):
            return safe_basename(value)
        return ABSOLUTE_PRIVATE_PATH.sub("<redacted-path>", value)
    return value


def public_json_bytes(raw):
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("upstream JSON exceeds the public proxy limit")
    value = json.loads(raw)
    public = sanitize_public_value(value)
    encoded = json.dumps(public, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if ABSOLUTE_PRIVATE_PATH.search(encoded.decode("utf-8")):
        raise ValueError("public JSON still contains an absolute private path")
    return encoded


def public_sse_line(line):
    if not line.startswith(b"data:"):
        return line
    prefix, raw = line.split(b":", 1)
    newline = b"\n" if raw.endswith(b"\n") else b""
    raw = raw.strip()
    if not raw:
        return line
    try:
        return prefix + b":" + public_json_bytes(raw) + newline
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None


class PublicWatcherProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream_host = "127.0.0.1"
    upstream_port = 8786
    upstream_timeout_secs = 60.0

    def do_GET(self):
        self.proxy(send_body=True)

    def do_HEAD(self):
        self.proxy(send_body=False)

    def do_POST(self):
        self.send_error(405, "read-only watcher")

    do_DELETE = do_POST
    do_PATCH = do_POST
    do_PUT = do_POST

    def proxy(self, send_body):
        parsed = urlsplit(self.path)
        if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
            self.send_error(400, "invalid request target")
            return
        if parsed.path.startswith("/api/") and parsed.path not in PUBLIC_API_GETS:
            self.send_error(404, "unknown public watcher endpoint")
            return
        target = parsed.path
        if parsed.query:
            target += "?" + parsed.query

        request_headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() in COPY_REQUEST_HEADERS
        }
        request_headers["Accept-Encoding"] = "identity"
        connection = HTTPConnection(
            self.upstream_host,
            self.upstream_port,
            timeout=self.upstream_timeout_secs,
        )
        response_started = False
        try:
            connection.request(self.command, target, headers=request_headers)
            upstream = connection.getresponse()
            content_type = upstream.getheader("Content-Type", "")
            is_json = (
                content_type.lower().startswith("application/json")
                and upstream.status not in {204, 304}
            )
            is_sse_path = parsed.path == "/api/v1/events" and upstream.status == 200
            if is_sse_path and not content_type.startswith("text/event-stream"):
                raise ValueError("event endpoint returned an unsafe content type")
            is_sse = is_sse_path

            if is_json and send_body:
                raw = upstream.read(MAX_JSON_BYTES + 1)
                body = public_json_bytes(raw)
                self.send_response(upstream.status)
                response_started = True
                self.copy_response_headers(upstream.getheaders(), transformed=True)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(upstream.status)
            response_started = True
            self.copy_response_headers(
                upstream.getheaders(), transformed=is_sse or is_json
            )
            if is_sse:
                self.close_connection = True
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("Connection", "close")
            elif is_json:
                self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not send_body:
                return

            if is_sse and content_type.startswith("text/event-stream"):
                while True:
                    line = upstream.readline()
                    if not line:
                        break
                    public_line = public_sse_line(line)
                    if public_line is None:
                        break
                    self.wfile.write(public_line)
                    self.wfile.flush()
                return

            while True:
                chunk = upstream.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionError, OSError, ValueError):
            if not response_started and not self.wfile.closed:
                try:
                    self.send_error(502, "watcher upstream unavailable")
                except (BrokenPipeError, ConnectionError, OSError):
                    pass
        finally:
            connection.close()

    def copy_response_headers(self, headers, transformed):
        for key, value in headers:
            lower = key.lower()
            if lower in HOP_BY_HOP_HEADERS:
                continue
            if transformed and lower in {"content-encoding", "content-length", "etag"}:
                continue
            if lower == "content-type" and transformed:
                continue
            self.send_header(key, value)

    def log_message(self, message, *args):
        try:
            print("watcher proxy: " + message % args, file=sys.stderr, flush=True)
        except OSError:
            pass


class PublicWatcherProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(self, server_address, handler_class, max_requests=64):
        self.request_slots = threading.BoundedSemaphore(max_requests)
        super().__init__(server_address, handler_class)

    def process_request(self, request, client_address):
        if not self.request_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.request_slots.release()


def network_address(value, flag, allow_private):
    parsed = urlsplit("//" + value)
    if parsed.port is None or parsed.hostname is None:
        raise argparse.ArgumentTypeError("{} must include an explicit host and port".format(flag))
    if parsed.hostname == "localhost":
        return parsed.hostname, parsed.port
    try:
        address = ipaddress.IPv4Address(parsed.hostname)
    except ipaddress.AddressValueError as error:
        raise argparse.ArgumentTypeError(
            "{} must use localhost or an explicit IPv4 address".format(flag)
        ) from error
    private_listener = allow_private and any(
        address in network for network in PRIVATE_LISTENER_NETWORKS
    )
    allowed = address.is_loopback or private_listener
    if address.is_unspecified or not allowed:
        scope = "loopback or private" if allow_private else "loopback"
        raise argparse.ArgumentTypeError(
            "{} must use an explicit {} address".format(flag, scope)
        )
    return parsed.hostname, parsed.port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1:8787")
    parser.add_argument("--upstream", default="127.0.0.1:8786")
    parser.add_argument("--upstream-timeout-secs", type=float, default=60.0)
    args = parser.parse_args()
    if args.upstream_timeout_secs <= 0:
        parser.error("--upstream-timeout-secs must be positive")
    listen = network_address(args.listen, "--listen", allow_private=True)
    upstream = network_address(args.upstream, "--upstream", allow_private=False)
    if listen == upstream:
        parser.error("--listen and --upstream must differ")

    PublicWatcherProxyHandler.upstream_host = upstream[0]
    PublicWatcherProxyHandler.upstream_port = upstream[1]
    PublicWatcherProxyHandler.upstream_timeout_secs = args.upstream_timeout_secs
    server = PublicWatcherProxyServer(listen, PublicWatcherProxyHandler)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
