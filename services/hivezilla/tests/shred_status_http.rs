#![cfg(unix)]

use hivezilla::shred_status::{HEALTH_PATH, MAX_HTTP_HEADER_BYTES, STATUS_PATH};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

const ALLOWED_ORIGIN: &str = "https://watch.example";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
struct Response {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("response body should be JSON")
    }
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> Self {
        Self {
            child: Some(command.spawn().expect("spawn shred-status server")),
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .expect("child already consumed")
            .try_wait()
    }

    fn collect_exited(&mut self) -> Output {
        self.child
            .take()
            .expect("child already consumed")
            .wait_with_output()
            .expect("collect exited shred-status server")
    }

    fn terminate_and_wait(&mut self, timeout: Duration) -> Output {
        let child = self.child.as_mut().expect("child already consumed");
        // SAFETY: `id` is the PID of the live child exclusively owned by this guard.
        let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
        assert_eq!(
            result,
            0,
            "send SIGTERM to shred-status server: {}",
            io::Error::last_os_error()
        );

        let deadline = Instant::now() + timeout;
        loop {
            match self.try_wait().expect("poll shred-status server") {
                Some(_) => return self.collect_exited(),
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                None => {
                    let child = self.child.as_mut().expect("child already consumed");
                    let _ = child.kill();
                    panic!("shred-status server did not exit after SIGTERM within {timeout:?}");
                }
            }
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }

        // Never make a failed test hang while reaping its child.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if child.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read reserved address")
}

fn request(
    address: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
) -> io::Result<Response> {
    let mut raw = format!("{method} {target} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n");
    for (name, value) in headers {
        raw.push_str(name);
        raw.push_str(": ");
        raw.push_str(value);
        raw.push_str("\r\n");
    }
    raw.push_str("\r\n");
    raw_request(address, raw.as_bytes())
}

fn oversized_request(address: SocketAddr) -> io::Result<Response> {
    let prefix = b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Fill: ";
    assert!(prefix.len() < MAX_HTTP_HEADER_BYTES);
    let mut raw = Vec::with_capacity(MAX_HTTP_HEADER_BYTES);
    raw.extend_from_slice(prefix);
    raw.resize(MAX_HTTP_HEADER_BYTES, b'a');

    let mut stream = connect(address)?;
    stream.write_all(&raw)?;
    // The server permits exactly MAX_HTTP_HEADER_BYTES, then rejects this byte. Sending no
    // trailing bytes avoids a reset caused by closing a socket with unread request data.
    stream.write_all(b"a")?;
    stream.shutdown(Shutdown::Write)?;
    read_response(stream)
}

fn raw_request(address: SocketAddr, raw: &[u8]) -> io::Result<Response> {
    let mut stream = connect(address)?;
    stream.write_all(raw)?;
    stream.shutdown(Shutdown::Write)?;
    read_response(stream)
}

fn connect(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&address, IO_TIMEOUT)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    Ok(stream)
}

fn read_response(mut stream: TcpStream) -> io::Result<Response> {
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let header_end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP header end"))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP status line"))?;
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP header"))?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    Ok(Response {
        status,
        headers,
        body: raw[header_end + 4..].to_vec(),
    })
}

fn wait_until_ready(child: &mut ChildGuard, address: SocketAddr) -> Response {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if child
            .try_wait()
            .expect("poll shred-status startup")
            .is_some()
        {
            let output = child.collect_exited();
            panic!(
                "shred-status server exited during startup ({:?})\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        match request(address, "GET", HEALTH_PATH, &[]) {
            Ok(response) if response.status == 200 => return response,
            Ok(_) | Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Ok(response) => panic!("shred-status readiness returned HTTP {}", response.status),
            Err(error) => panic!("shred-status did not become ready: {error}"),
        }
    }
}

fn assert_security_headers(response: &Response) {
    assert_eq!(
        response.header("content-type"),
        Some("application/json; charset=utf-8")
    );
    assert_eq!(response.header("cache-control"), Some("no-store"));
    assert_eq!(
        response.header("content-security-policy"),
        Some("default-src 'none'; frame-ancestors 'none'")
    );
    assert_eq!(response.header("referrer-policy"), Some("no-referrer"));
    assert_eq!(response.header("x-content-type-options"), Some("nosniff"));
    assert_eq!(response.header("x-frame-options"), Some("DENY"));
    assert_eq!(response.header("connection"), Some("close"));
}

#[test]
fn real_cli_serves_bounded_read_only_http_and_stops_on_sigterm() {
    let temp = TempDir::new().expect("create temporary directory");
    let missing_status = temp.path().join("missing-private-status.json");
    assert!(!missing_status.exists());

    // Keeping this listener open but unaccepted makes the receiver reachable yet unable to
    // answer. The configured total timeout must turn that into an unavailable source promptly.
    let receiver = TcpListener::bind("127.0.0.1:0").expect("reserve receiver port");
    let receiver_address = receiver.local_addr().expect("read receiver address");
    let listen_address = reserve_address();

    let mut command = Command::new(env!("CARGO_BIN_EXE_hivezilla"));
    command
        .args([
            "serve-shred-status",
            "--listen",
            &listen_address.to_string(),
            "--hivezilla-status-file",
            missing_status.to_str().expect("temporary path is UTF-8"),
            "--receiver-metrics-url",
            &format!("http://{receiver_address}/metrics"),
            "--cors-origin",
            ALLOWED_ORIGIN,
            "--receiver-timeout-secs",
            "0.1",
            "--interval-secs",
            "60",
        ])
        .env("RUST_LOG", "off")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for variable in [
        "SHRED_STATUS_LISTEN",
        "SHRED_STATUS_HIVEZILLA_FILE",
        "SHRED_STATUS_RECEIVER_METRICS_URL",
        "SHRED_STATUS_OUTPUT_FILE",
        "SHRED_STATUS_CORS_ORIGIN",
        "SHRED_STATUS_INTERVAL_SECS",
        "SHRED_STATUS_RECEIVER_TIMEOUT_SECS",
        "SHRED_STATUS_HIVEZILLA_STALE_AFTER_SECS",
        "SHRED_STATUS_TVU_ACTIVE_AFTER_SECS",
    ] {
        command.env_remove(variable);
    }
    let mut child = ChildGuard::spawn(&mut command);

    let health = wait_until_ready(&mut child, listen_address);
    assert_security_headers(&health);
    let health_json = health.json();
    assert_eq!(health_json["ok"], true);
    assert_eq!(health_json["receiver"], "unavailable");
    assert_eq!(health_json["hivezilla"], "unavailable");

    let get = request(listen_address, "GET", STATUS_PATH, &[]).expect("GET status");
    assert_eq!(get.status, 200);
    assert_security_headers(&get);
    let get_json = get.json();
    assert_eq!(get_json["schema_version"], 1);
    assert_eq!(get_json["gossip"]["state"], "unavailable");
    assert_eq!(get_json["tvu"]["state"], "unavailable");
    assert_eq!(get_json["forwarding"]["state"], "unavailable");
    assert_eq!(get_json["hivezilla"]["availability"], "unavailable");

    let head = request(listen_address, "HEAD", STATUS_PATH, &[]).expect("HEAD status");
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty());
    assert_eq!(
        head.header("content-length"),
        Some(get.body.len().to_string().as_str())
    );

    let exact_origin = request(
        listen_address,
        "GET",
        STATUS_PATH,
        &[("Origin", ALLOWED_ORIGIN)],
    )
    .expect("GET with exact origin");
    assert_eq!(exact_origin.status, 200);
    assert_eq!(
        exact_origin.header("access-control-allow-origin"),
        Some(ALLOWED_ORIGIN)
    );
    assert_eq!(exact_origin.header("vary"), Some("Origin"));
    assert_eq!(
        exact_origin.header("access-control-allow-credentials"),
        None
    );

    let evil_origin = request(
        listen_address,
        "GET",
        STATUS_PATH,
        &[("Origin", "https://watch.example.evil")],
    )
    .expect("GET with evil origin");
    assert_eq!(evil_origin.status, 403);
    assert_eq!(evil_origin.json()["error"], "origin not allowed");
    assert_eq!(evil_origin.header("access-control-allow-origin"), None);

    let duplicate_origin = request(
        listen_address,
        "GET",
        STATUS_PATH,
        &[("Origin", ALLOWED_ORIGIN), ("Origin", ALLOWED_ORIGIN)],
    )
    .expect("GET with duplicate origin");
    assert_eq!(duplicate_origin.status, 403);
    assert_eq!(duplicate_origin.json()["error"], "origin not allowed");
    assert_eq!(duplicate_origin.header("access-control-allow-origin"), None);

    let options = request(
        listen_address,
        "OPTIONS",
        STATUS_PATH,
        &[
            ("Origin", ALLOWED_ORIGIN),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "Accept, Cache-Control"),
        ],
    )
    .expect("OPTIONS preflight");
    assert_eq!(options.status, 204);
    assert!(options.body.is_empty());
    assert_eq!(options.header("allow"), Some("GET, HEAD, OPTIONS"));
    assert_eq!(
        options.header("access-control-allow-origin"),
        Some(ALLOWED_ORIGIN)
    );
    assert_eq!(options.header("vary"), Some("Origin"));
    assert_eq!(
        options.header("access-control-allow-methods"),
        Some("GET, HEAD, OPTIONS")
    );
    assert_eq!(
        options.header("access-control-allow-headers"),
        Some("Accept, Cache-Control")
    );
    assert_eq!(options.header("access-control-max-age"), Some("600"));

    let propfind = request(listen_address, "PROPFIND", STATUS_PATH, &[]).expect("PROPFIND");
    assert_eq!(propfind.status, 405);
    assert_eq!(propfind.header("allow"), Some("GET, HEAD, OPTIONS"));
    assert_eq!(propfind.json()["error"], "read-only status service");

    let query = request(
        listen_address,
        "GET",
        &format!("{STATUS_PATH}?secret=must-not-echo"),
        &[],
    )
    .expect("GET with query");
    assert_eq!(query.status, 400);
    assert_eq!(query.json()["error"], "invalid request target");
    assert!(!String::from_utf8_lossy(&query.body).contains("must-not-echo"));

    let oversized = oversized_request(listen_address).expect("oversized request");
    assert_eq!(oversized.status, 431);
    assert_eq!(oversized.json()["error"], "request headers too large");

    let output = child.terminate_and_wait(Duration::from_secs(5));
    assert!(
        output.status.success(),
        "SIGTERM exit was {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
