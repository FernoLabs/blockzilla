use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use blockzilla_compact_v2_reader::manifest::GENERATION_MANIFEST_FILE;
use sha2::{Digest, Sha256};

pub(super) fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(super) struct MockGateway {
    pub(super) base_url: String,
    stop: Arc<AtomicBool>,
    address: std::net::SocketAddr,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockGateway {
    pub(super) fn start(root: &Path, epoch: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let root = root.to_path_buf();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => serve_request(stream, &root, epoch),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            base_url: format!("http://{address}"),
            stop,
            address,
            thread: Some(thread),
        }
    }
}

impl Drop for MockGateway {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve_request(mut stream: TcpStream, root: &Path, epoch: u64) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut buffer).unwrap();
        if count == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..count]);
    }
    let request = String::from_utf8(request).unwrap();
    let mut lines = request.split("\r\n");
    let first = lines.next().unwrap();
    let mut first_parts = first.split_whitespace();
    let method = first_parts.next().unwrap();
    let path = first_parts.next().unwrap();
    let prefix = format!("/v1/epochs/{epoch}/");
    let object = if path == format!("{prefix}manifest") {
        Some(GENERATION_MANIFEST_FILE)
    } else {
        path.strip_prefix(&format!("{prefix}files/"))
    };
    let Some(object) = object else {
        write_response(&mut stream, 404, &[], None, None);
        return;
    };
    let Ok(bytes) = fs::read(root.join(object)) else {
        write_response(&mut stream, 404, &[], None, None);
        return;
    };
    let etag = format!("\"{}\"", hex_lower(&Sha256::digest(&bytes)));
    if method == "HEAD" {
        write_response(
            &mut stream,
            200,
            &[],
            Some((0, 0, bytes.len(), bytes.len())),
            Some(&etag),
        );
        return;
    }
    if object == GENERATION_MANIFEST_FILE {
        write_response(&mut stream, 200, &bytes, None, Some(&etag));
        return;
    }
    let (start, end) = lines
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            let range = lower.strip_prefix("range: bytes=")?;
            let (start, end) = range.split_once('-')?;
            Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
        })
        .unwrap();
    let body = &bytes[start..=end];
    write_response(
        &mut stream,
        206,
        body,
        Some((start, end, bytes.len(), body.len())),
        Some(&etag),
    );
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    range: Option<(usize, usize, usize, usize)>,
    etag: Option<&str>,
) {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        _ => "Not Found",
    };
    let content_length = range.map(|value| value.3).unwrap_or(body.len());
    let mut header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {content_length}\r\nConnection: close\r\n"
    );
    if let Some(etag) = etag {
        header.push_str(&format!("ETag: {etag}\r\n"));
    }
    if status == 206
        && let Some((start, end, total, _)) = range
    {
        header.push_str(&format!("Content-Range: bytes {start}-{end}/{total}\r\n"));
    }
    header.push_str("\r\n");
    stream.write_all(header.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
}
