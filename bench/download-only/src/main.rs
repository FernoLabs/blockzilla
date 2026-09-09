//! HTTP transport ceiling: discard bodies, keep exact range and identity checks.
use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, ValueEnum};
use reqwest::{Version, blocking::Client, header};
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const MIB: u64 = 1024 * 1024;
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Protocol {
    Http1,
    Http2,
    Http2Adaptive,
}
#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long, value_enum, default_value = "http1")]
    protocol: Protocol,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    /// Independent connection pools; one pool permits HTTP/2 multiplexing.
    #[arg(long, default_value_t = 1)]
    clients: usize,
    #[arg(long, default_value_t = 32)]
    range_mib: u64,
    #[arg(long, default_value_t=512 * MIB)]
    bytes: u64,
    #[arg(long, default_value_t = 0)]
    offset: u64,
    /// Hash fixed 1 MiB pieces, independent of request boundaries. Use for validation.
    #[arg(long)]
    hash: bool,
    #[arg(long)]
    output: PathBuf,
}
fn value(response: &reqwest::blocking::Response, name: &str) -> Result<String> {
    Ok(response
        .headers()
        .get(name)
        .with_context(|| format!("missing {name}"))?
        .to_str()?
        .to_owned())
}
fn version(p: Protocol) -> Version {
    match p {
        Protocol::Http1 => Version::HTTP_11,
        _ => Version::HTTP_2,
    }
}
fn head(client: &Client, a: &Args) -> Result<(u64, String)> {
    let r = client
        .head(&a.url)
        .header(header::ACCEPT_ENCODING, "identity")
        .send()?;
    ensure!(r.status() == 200, "HEAD status {}", r.status());
    ensure!(
        r.version() == version(a.protocol),
        "requested protocol not negotiated: {:?}",
        r.version()
    );
    let size = value(&r, "content-length")?.parse()?;
    let etag = value(&r, "etag")?;
    ensure!(
        etag.starts_with('"') && etag.ends_with('"'),
        "strong ETag required"
    );
    Ok((size, etag))
}
fn run(a: &Args) -> Result<Value> {
    ensure!((1..=16).contains(&a.workers), "workers must be 1..16");
    ensure!(
        a.clients > 0 && a.clients <= a.workers,
        "clients must be 1..workers"
    );
    ensure!((1..=64).contains(&a.range_mib), "range-mib must be 1..64");
    ensure!(
        a.bytes > 0 && a.bytes <= 16 * 1024 * MIB && a.bytes.is_multiple_of(MIB),
        "bytes must be whole MiB, at most 16 GiB"
    );
    ensure!(a.offset.is_multiple_of(MIB), "offset must be MiB aligned");
    let url = reqwest::Url::parse(&a.url)?;
    ensure!(
        (url.scheme() == "https"
            || (cfg!(test) && url.scheme() == "http" && url.host_str() == Some("127.0.0.1")))
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "public HTTPS URL without credentials or query required"
    );
    let started = Instant::now();
    let mut clients = Vec::new();
    for _ in 0..a.clients {
        let b = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(90))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .retry(reqwest::retry::never())
            .user_agent("blockzilla-download-only/1");
        let b = match a.protocol {
            Protocol::Http1 => b.http1_only(),
            Protocol::Http2 => b.http2_prior_knowledge(),
            Protocol::Http2Adaptive => b.http2_prior_knowledge().http2_adaptive_window(true),
        };
        clients.push(b.build()?);
    }
    let identity = head(&clients[0], a)?;
    ensure!(
        a.offset
            .checked_add(a.bytes)
            .is_some_and(|end| end <= identity.0),
        "selected range exceeds object"
    );
    for c in &clients[1..] {
        ensure!(head(c, a)? == identity, "identity changed between clients");
    }
    let setup_s = started.elapsed().as_secs_f64();
    let range = a.range_mib * MIB;
    let count = a.bytes.div_ceil(range) as usize;
    let next = AtomicUsize::new(0);
    let stopped = AtomicBool::new(false);
    let received = AtomicU64::new(0);
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let reports = Mutex::new(Vec::with_capacity(count));
    let errors = Mutex::new(Vec::new());
    let scan = Instant::now();
    thread::scope(|scope| {
        for worker in 0..a.workers {
            let client = &clients[worker % a.clients];
            let (next, stopped, received, active, peak, reports, errors, identity) = (
                &next, &stopped, &received, &active, &peak, &reports, &errors, &identity,
            );
            scope.spawn(move || {
                let mut buffer=vec![0_u8;MIB as usize];
                while !stopped.load(Ordering::Relaxed) {
                    let index=next.fetch_add(1,Ordering::Relaxed);
                    if index>=count { break; }
                    let offset=a.offset+index as u64*range;
                    let length=range.min(a.offset+a.bytes-offset);
                    peak.fetch_max(active.fetch_add(1,Ordering::Relaxed)+1,Ordering::Relaxed);
                    let result=(|| -> Result<Value> {
                        let start=Instant::now();
                        let mut r=client.get(&a.url).header(header::ACCEPT_ENCODING,"identity")
                            .header(header::IF_MATCH,&identity.1)
                            .header(header::RANGE,format!("bytes={offset}-{}",offset+length-1)).send()?;
                        let header_s=start.elapsed().as_secs_f64();
                        ensure!(r.status()==206,"range status {}",r.status());
                        ensure!(r.version()==version(a.protocol),"unexpected protocol {:?}",r.version());
                        ensure!(value(&r,"etag")?==identity.1,"ETag changed");
                        ensure!(value(&r,"content-length")?.parse::<u64>()?==length,"wrong Content-Length");
                        ensure!(value(&r,"content-range")?==format!("bytes {offset}-{}/{}",offset+length-1,identity.0),"wrong Content-Range");
                        ensure!(r.headers().get(header::CONTENT_ENCODING).is_none_or(|v|v=="identity"),"encoded body");
                        let cf_ray=r.headers().get("cf-ray").and_then(|h|h.to_str().ok()).map(str::to_owned);
                        let cache=r.headers().get("cf-cache-status").and_then(|h|h.to_str().ok()).map(str::to_owned);
                        let body=Instant::now();
                        let mut hashes=Vec::new();
                        let mut completed=0;
                        while completed<length {
                            let size=(length-completed).min(MIB) as usize;
                            let mut filled=0;
                            while filled<size {
                                let n=r.read(&mut buffer[filled..size])?;
                                ensure!(n!=0,"short body at {} of {length}",completed+filled as u64);
                                received.fetch_add(n as u64,Ordering::Relaxed);
                                filled+=n;
                            }
                            if a.hash { hashes.push(blake3::hash(&buffer[..size]).to_hex().to_string()); }
                            completed+=size as u64;
                        }
                        let mut extra=[0_u8;1];
                        let n=r.read(&mut extra)?;
                        received.fetch_add(n as u64,Ordering::Relaxed);
                        ensure!(n==0,"body exceeds requested range");
                        Ok(json!({"index":index,"offset":offset,"bytes":length,"worker":worker,"client":worker%a.clients,"protocol":format!("{:?}",r.version()),"header_s":header_s,"body_s":body.elapsed().as_secs_f64(),"seconds":start.elapsed().as_secs_f64(),"cf_ray":cf_ray,"cache_status":cache,"piece_blake3":hashes}))
                    })();
                    active.fetch_sub(1,Ordering::Relaxed);
                    match result { Ok(r)=>reports.lock().unwrap().push(r), Err(e)=>{ stopped.store(true,Ordering::Relaxed);errors.lock().unwrap().push(format!("range {index}: {e:#}"));break; } }
                }
            });
        }
    });
    let scan_s = scan.elapsed().as_secs_f64();
    let mut reports = reports.into_inner().unwrap();
    reports.sort_by_key(|v| v["index"].as_u64().unwrap());
    let mut errors = errors.into_inner().unwrap();
    match head(&clients[0], a) {
        Ok(after) if after == identity => {}
        Ok(_) => errors.push("post-scan identity changed".into()),
        Err(e) => errors.push(format!("post-scan HEAD: {e:#}")),
    }
    let bytes = received.load(Ordering::Relaxed);
    let valid = errors.is_empty() && reports.len() == count && bytes == a.bytes;
    Ok(
        json!({"schema":"blockzilla-download-only-v1","valid":valid,"url":a.url,"protocol_requested":format!("{:?}",a.protocol),"workers":a.workers,"clients":a.clients,"range_bytes":range,"offset":a.offset,"expected_bytes":a.bytes,"received_bytes":bytes,"object_size":identity.0,"etag":identity.1,"hash_enabled":a.hash,"setup_s":setup_s,"scan_s":scan_s,"total_s":setup_s+scan_s,"scan_MBps":if valid {Some(bytes as f64/1e6/scan_s)}else{None},"peak_active_requests":peak.load(Ordering::Relaxed),"application_buffer_bytes":a.workers as u64*MIB,"requests":reports,"errors":errors}),
    )
}
fn main() -> Result<()> {
    let a = Args::parse();
    // Reserve a fresh receipt before any network read; failed runs stay visible.
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&a.output)?;
    let report = match run(&a) {
        Ok(r) => r,
        Err(e) => {
            json!({"schema":"blockzilla-download-only-v1","valid":false,"error":format!("{e:#}")})
        }
    };
    serde_json::to_writer_pretty(&mut out, &report)?;
    out.write_all(b"\n")?;
    println!(
        "{}",
        json!({"valid":report["valid"],"scan_MBps":report["scan_MBps"],"scan_s":report["scan_s"],"received_bytes":report["received_bytes"],"output":a.output})
    );
    if report["valid"] != true {
        bail!("download failed; see receipt");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    // A real HTTP fixture checks receipt acceptance and exact body consumption.
    fn fixture(fault: &'static str, range_mib: u64) -> Value {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/object", listener.local_addr().unwrap());
        let done = AtomicBool::new(false);
        thread::scope(|scope| {
            let d = &done;
            let server=scope.spawn(move || {
                while !d.load(Ordering::Relaxed) {
                    let (mut stream,_)=match listener.accept() { Ok(s)=>s,Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>{thread::sleep(Duration::from_millis(1));continue},Err(e)=>panic!("{e}") };
                    stream.set_nonblocking(false).unwrap();
                    stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                    stream.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                    let mut request=Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut b=[0];if stream.read(&mut b).unwrap_or(0)==0 {break} request.push(b[0]);
                    }
                    let request=String::from_utf8(request).unwrap();
                    if request.is_empty() { continue; }
                    if request.starts_with("HEAD") {
                        let _=write!(stream,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n",2*MIB);
                        continue;
                    }
                    let line=request.lines().find(|l|l.to_ascii_lowercase().starts_with("range:")).unwrap();
                    let (lo,hi)=line.split_once("bytes=").unwrap().1.trim().split_once('-').unwrap();
                    let lo=lo.parse::<u64>().unwrap();let hi=hi.parse::<u64>().unwrap();let len=hi-lo+1;
                    let etag=if fault=="etag" {"changed"} else {"fixture"};
                    let start=if fault=="range" {lo+1} else {lo};
                    let _=write!(stream,"HTTP/1.1 206 Partial Content\r\nContent-Length: {len}\r\nContent-Range: bytes {start}-{hi}/{}\r\nETag: \"{etag}\"\r\nConnection: close\r\n\r\n",2*MIB);
                    let sent=if fault=="short" {len/2} else {len};
                    let _=stream.write_all(&vec![19_u8;sent as usize]);
                }
            });
            let a = Args {
                url,
                protocol: Protocol::Http1,
                workers: 2,
                clients: 1,
                range_mib,
                bytes: 2 * MIB,
                offset: 0,
                hash: true,
                output: PathBuf::new(),
            };
            let result = run(&a);
            done.store(true, Ordering::Relaxed);
            server.join().unwrap();
            result.unwrap()
        })
    }
    #[test]
    fn identical_hashes_across_request_boundaries() {
        let a = fixture("", 1);
        let b = fixture("", 2);
        assert_eq!(a["valid"], true, "{a:#}");
        assert_eq!(b["valid"], true, "{b:#}");
        let pieces = |r: &Value| {
            r["requests"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|v| v["piece_blake3"].as_array().unwrap().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(pieces(&a), pieces(&b));
        assert_eq!(a["received_bytes"], 2 * MIB);
    }
    #[test]
    fn incomplete_or_changed_input_has_no_accepted_throughput() {
        for fault in ["short", "etag", "range"] {
            let result = fixture(fault, 1);
            assert_eq!(result["valid"], false, "{fault}");
            assert!(result["scan_MBps"].is_null());
            assert!(!result["errors"].as_array().unwrap().is_empty());
        }
    }
}
