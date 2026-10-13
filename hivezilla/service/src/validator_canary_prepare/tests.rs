use super::*;
use std::{net::TcpListener, os::unix::fs::symlink, thread};

fn private_temp() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

fn asset(body: &[u8], url: String) -> Asset {
    Asset {
        name: "asset.bin".into(),
        size: body.len() as u64,
        digest: Some(format!(
            "sha256:{}",
            Sha256::digest(body)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )),
        browser_download_url: url,
    }
}

fn http_once(body: Vec<u8>, advertised: usize) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0; 1];
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            assert!(request.len() < 16 * 1024);
        }
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {advertised}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    });
    (format!("http://{address}/asset.bin"), server)
}

#[test]
fn pinned_assets_require_exact_stable_tag_url_size_and_digest() {
    assert_eq!(
        release_url("anza-xyz/agave", AGAVE).unwrap().as_str(),
        "https://api.github.com/repos/anza-xyz/agave/releases/tags/v4.3.0"
    );
    let current = release_url("rpcpool/yellowstone-grpc", YELLOWSTONE).unwrap();
    assert_eq!(
        current.path(),
        "/repos/rpcpool/yellowstone-grpc/releases/tags/v16.0.0+solana.4.3.0"
    );
    let mut item = asset(
        b"payload",
        format!("https://github.com/anza-xyz/agave/releases/download/{AGAVE}/asset.bin"),
    );
    let mut release = Release {
        tag_name: AGAVE.into(),
        prerelease: false,
        draft: false,
        assets: vec![item.clone()],
    };
    assert!(select_asset(&release, "anza-xyz/agave", AGAVE, "asset.bin").is_ok());
    release.prerelease = true;
    assert!(select_asset(&release, "anza-xyz/agave", AGAVE, "asset.bin").is_err());
    release.prerelease = false;
    release.assets.push(item.clone());
    assert!(select_asset(&release, "anza-xyz/agave", AGAVE, "asset.bin").is_err());
    release.assets.pop();
    for url in [
        "http://github.com/anza-xyz/agave/releases/download/v4.3.0/asset.bin",
        "https://github.com/other/agave/releases/download/v4.3.0/asset.bin",
        "https://github.com/anza-xyz/agave/releases/download/v4.3.0/asset.bin?token=secret",
    ] {
        release.assets[0].browser_download_url = url.into();
        assert!(select_asset(&release, "anza-xyz/agave", AGAVE, "asset.bin").is_err());
    }
    for digest in [
        None,
        Some("sha256:0".into()),
        Some(format!("sha256:{}", "G".repeat(64))),
    ] {
        item.digest = digest;
        assert!(item.digest().is_err());
    }
    item = asset(b"payload", String::new());
    item.size = MAX_ASSET_BYTES + 1;
    assert!(item.digest().is_err());
}

#[test]
fn configuration_matches_the_frozen_local_canary_contract() {
    let root = Path::new("/private/canary");
    let config = plugin_config(&root.join("releases/yellowstone-16"));
    assert_eq!(
        config["grpc"]["listen"],
        json!([{"address":"127.0.0.1:10000"}])
    );
    assert_eq!(config["prometheus"]["address"], "127.0.0.1:8999");
    assert_eq!(config["grpc"]["max_decoding_message_size"], "134_217_728");
    assert!(config["grpc"]["filters"].is_null());
    assert!(config["grpc"]["filter_limits"].is_null());
    assert_eq!(config["grpc"]["replay_stored_slots"], 512);
    let args = validator_argv(root, &root.join("validator"));
    let args = args.as_array().unwrap();
    assert_eq!(args[0], "/private/canary/validator");
    for flag in [
        "--no-voting",
        "--no-xdp",
        "--private-rpc",
        "--full-rpc-api",
        "--enable-rpc-transaction-history",
        "--enable-extended-tx-metadata-storage",
        "--only-known-rpc",
    ] {
        assert_eq!(args.iter().filter(|value| **value == flag).count(), 1);
    }
    assert_eq!(
        args.iter()
            .filter(|value| **value == "--entrypoint")
            .count(),
        5
    );
    assert_eq!(
        args.iter()
            .filter(|value| **value == "--known-validator")
            .count(),
        4
    );
    for pair in [
        ("--rpc-bind-address", "127.0.0.1"),
        ("--rpc-port", "8899"),
        ("--expected-genesis-hash", GENESIS),
    ] {
        assert!(
            args.windows(2)
                .any(|values| values[0] == pair.0 && values[1] == pair.1)
        );
    }
}

#[test]
fn config_publication_preserves_equal_bytes_and_rejects_different_or_symlink() {
    let dir = private_temp();
    let path = dir.path().join("config.json");
    fs::write(&path, b"{\"a\":1}\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    publish_json(&path, &json!({"a":1}), false).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"{\"a\":1}\n");
    assert!(publish_json(&path, &json!({"a":2}), false).is_err());
    let link = dir.path().join("link.json");
    symlink(&path, &link).unwrap();
    assert!(publish_json(&link, &json!({"a":1}), false).is_err());
    let fresh = dir.path().join("fresh.json");
    publish_json(&fresh, &json!({"a":2}), false).unwrap();
    assert_eq!(read_json(&fresh).unwrap(), json!({"a":2}));
    assert_eq!(fs::metadata(fresh).unwrap().mode() & 0o777, 0o600);
}

#[test]
fn report_update_only_changes_readiness_and_asset_order() {
    let dir = private_temp();
    let path = dir.path().join("install.json");
    let previous = json!({"schema":"hivezilla-validator-canary-install-v1", "root":"/canary", "validator_ready":false,
        "validator_binary":"/canary/validator", "assets":[{"name":"b","sha256":"2","bytes":2},{"name":"a","sha256":"1","bytes":1}]});
    publish_json(&path, &previous, false).unwrap();
    let mut next = previous.clone();
    next["validator_ready"] = json!(true);
    next["assets"].as_array_mut().unwrap().reverse();
    validate_report_update(&path, &next).unwrap();
    next["validator_binary"] = json!("/other/validator");
    assert!(validate_report_update(&path, &next).is_err());
    assert_eq!(read_json(&path).unwrap(), previous);
}

#[test]
fn private_roots_reject_links_shared_write_and_parent_paths() {
    let dir = private_temp();
    let root = dir.path().join("root");
    private_directory(&root).unwrap();
    assert!(private_directory(&root).is_ok());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o777)).unwrap();
    assert!(private_directory(&root).is_err());
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let link = dir.path().join("link");
    symlink(&root, &link).unwrap();
    assert!(root_directory(&link).is_err());
    assert!(root_directory(&root.join("../elsewhere")).is_err());
}

#[tokio::test]
async fn real_http_download_verifies_bytes_reuses_exact_file_and_preserves_mismatch() {
    let dir = private_temp();
    let body = b"verified release bytes".to_vec();
    let (url, server) = http_once(body.clone(), body.len());
    let item = asset(&body, url);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let output = download_asset(&client, &item, dir.path()).await.unwrap();
    server.join().unwrap();
    assert_eq!(fs::read(&output).unwrap(), body);
    // No server remains. Success proves exact files do not trigger another request.
    assert_eq!(
        download_asset(&client, &item, dir.path()).await.unwrap(),
        output
    );
    fs::write(&output, b"different").unwrap();
    assert!(download_asset(&client, &item, dir.path()).await.is_err());
    assert_eq!(fs::read(output).unwrap(), b"different");
}

#[tokio::test]
async fn short_or_wrong_http_body_never_publishes_an_asset() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for short in [true, false] {
        let dir = private_temp();
        let body = b"release".to_vec();
        let (url, server) = http_once(
            body.clone(),
            if short { body.len() + 1 } else { body.len() },
        );
        let mut item = asset(&body, url);
        if !short {
            item.digest = Some(format!("sha256:{}", "0".repeat(64)));
        }
        assert!(download_asset(&client, &item, dir.path()).await.is_err());
        server.join().unwrap();
        assert!(!dir.path().join("asset.bin").exists());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn exact_tar_members_preserve_existing_files_and_reject_other_members() {
    let dir = private_temp();
    let source = dir.path().join("source");
    fs::create_dir_all(source.join("solana-release/bin")).unwrap();
    for name in ["solana", "solana-keygen"] {
        fs::write(
            source.join("solana-release/bin").join(name),
            format!("\x7fELF{name}"),
        )
        .unwrap();
    }
    fs::write(source.join("unrelated"), b"must not be extracted").unwrap();
    let archive = dir.path().join("release.tar.bz2");
    // Use the same exact member spelling as the official release.
    assert!(
        std::process::Command::new("tar")
            .arg("-cjf")
            .arg(&archive)
            .arg("-C")
            .arg(&source)
            .args(["solana-release", "unrelated"])
            .status()
            .unwrap()
            .success()
    );
    let output = dir.path().join("solana");
    extract_tool(&archive, &output, "solana").await.unwrap();
    assert_eq!(fs::read(&output).unwrap(), b"\x7fELFsolana");
    extract_tool(&archive, &output, "solana").await.unwrap();
    assert!(!dir.path().join("unrelated").exists());
    assert!(extract_tool(&archive, &output, "unrelated").await.is_err());
    fs::write(&output, b"different existing tool").unwrap();
    assert!(extract_tool(&archive, &output, "solana").await.is_err());
    assert_eq!(fs::read(output).unwrap(), b"different existing tool");
}

#[tokio::test]
async fn child_timeout_stops_only_its_process_group() {
    let output = tempfile::tempfile().unwrap();
    let start = Instant::now();
    let result = checked_command(
        Path::new("/bin/sh"),
        &["-c".as_ref(), "sleep 30".as_ref()],
        &output,
        1024,
        Duration::from_millis(100),
    )
    .await;
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_secs(3));
}
