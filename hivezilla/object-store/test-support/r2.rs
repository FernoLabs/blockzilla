//! Small local HTTP fixture shared by backend and service command integration tests.
#![allow(dead_code)]

use hivezilla_object_store::uploader::{Provider, StorageSettings};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

#[derive(Default)]
pub struct State {
    pub objects: BTreeMap<String, Vec<u8>>,
    pub requests: Vec<(String, String, BTreeMap<String, String>, Vec<u8>)>,
    pub fail_put: bool,
    pub drop_put_response: bool,
    pub corrupt_get: bool,
    pub encoded_get: bool,
    pub short_get: bool,
    pub large_head: bool,
    pub concurrent_put: Option<Vec<u8>>,
}

pub struct Server {
    pub endpoint: String,
    pub state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_state = Arc::clone(&state);
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => handle(stream, &thread_state),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            }
        });
        Self {
            endpoint,
            state,
            stop,
            thread: Some(thread),
        }
    }

    pub fn settings(&self) -> StorageSettings {
        StorageSettings {
            endpoint: self.endpoint.clone(),
            region: "auto".into(),
            bucket: "bucket".into(),
            access_key: "test-access".into(),
            secret_key: "test-secret".into(),
            provider: Provider::R2,
            session_token: None,
        }
    }

    pub fn count(&self, method: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|(actual, _, _, _)| actual == method)
            .count()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let result = thread.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}

fn handle(mut stream: TcpStream, state: &Mutex<State>) {
    // Accepted sockets can inherit the listener's nonblocking flag on BSD/macOS.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap() == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap().to_owned();
    let key = parts
        .next()
        .unwrap()
        .strip_prefix("/bucket/")
        .unwrap()
        .to_owned();
    let mut headers = BTreeMap::new();
    let mut header_bytes = 0;
    loop {
        line.clear();
        header_bytes += reader.read_line(&mut line).unwrap();
        assert!(header_bytes < 32 * 1024);
        if line == "\r\n" {
            break;
        }
        let (key, value) = line.trim_end().split_once(':').unwrap();
        headers.insert(key.to_ascii_lowercase(), value.trim().to_owned());
    }
    let length = headers
        .get("content-length")
        .map_or(0, |value| value.parse::<usize>().unwrap());
    assert!(length < 2 * 1024 * 1024);
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).unwrap();
    assert!(
        headers
            .get("authorization")
            .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256 "))
    );
    let mut state = state.lock().unwrap();
    state
        .requests
        .push((method.clone(), key.clone(), headers.clone(), bytes.clone()));
    let (status, mut body) = if method == "PUT" {
        assert_eq!(headers.get("if-none-match").map(String::as_str), Some("*"));
        if state.fail_put {
            (
                503,
                b"<Error><Code>ServiceUnavailable</Code><Message>secret-response</Message></Error>"
                    .to_vec(),
            )
        } else {
            if let Some(other) = state.concurrent_put.take() {
                state.objects.insert(key.clone(), other);
            }
            if state.objects.contains_key(&key) {
                (412, Vec::new())
            } else {
                state.objects.insert(key.clone(), bytes);
                if std::mem::take(&mut state.drop_put_response) {
                    return;
                }
                (200, Vec::new())
            }
        }
    } else if let Some(bytes) = state.objects.get(&key) {
        (200, bytes.clone())
    } else {
        (404, Vec::new())
    };
    if method == "GET" && state.corrupt_get && !body.is_empty() {
        body[0] ^= 1;
    }
    let declared_length = if method == "HEAD" && state.large_head {
        u64::MAX
    } else {
        body.len() as u64
    };
    let extra = if method == "GET" && state.encoded_get {
        "Content-Encoding: gzip\r\n"
    } else {
        ""
    };
    if method == "GET" && state.short_get {
        body.truncate(body.len() / 2);
    }
    let head = format!(
        "HTTP/1.1 {status} Test\r\nContent-Length: {declared_length}\r\n{extra}x-amz-meta-sha256: untrusted-metadata\r\nConnection: close\r\n\r\n"
    );
    // The client can reject a response before reading its body.
    if stream.write_all(head.as_bytes()).is_ok() && method != "HEAD" {
        let _ = stream.write_all(&body);
    }
}
