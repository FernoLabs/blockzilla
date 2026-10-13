#[path = "../test-support/r2.rs"]
mod support;

use hivezilla_object_store::{
    CreateDisposition, CreateRequest, Error, ImmutableObjectStore, ObjectKey, r2::R2ObjectStore,
    uploader::S3Client,
};
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use support::Server;

fn store(server: &Server) -> R2ObjectStore {
    R2ObjectStore::new(
        S3Client::new(server.settings(), 0).unwrap(),
        "reader-a".into(),
        1024 * 1024,
    )
    .unwrap()
}

fn request(bytes: &[u8]) -> CreateRequest {
    CreateRequest {
        key: ObjectKey::new("ranges/first.hseg").unwrap(),
        encoded_len: bytes.len() as u64,
        encoded_sha256: Sha256::digest(bytes).into(),
    }
}

#[test]
fn r2_upload_replay_and_readback_preserve_exact_bytes() {
    let server = Server::start();
    let store = store(&server);
    let bytes = b"exact source bytes";
    let request = request(bytes);
    let created = store.create_if_absent(&request, &mut &bytes[..]).unwrap();
    assert_eq!(created.disposition, CreateDisposition::Created);
    assert_eq!(created.metadata.provider_sha256, None);
    assert_eq!(created.metadata.version, None);
    let verified = store.verify_exact(&request).unwrap();
    assert_eq!(verified.encoded_len, bytes.len() as u64);
    assert!(
        server.count("GET") > 0,
        "user metadata must never replace readback"
    );
    let replay = store.create_if_absent(&request, &mut io::empty()).unwrap();
    assert_eq!(replay.disposition, CreateDisposition::AlreadyExisted);
    assert_eq!(server.count("PUT"), 1);
    let mut read = Vec::new();
    store
        .open(&request.key)
        .unwrap()
        .body
        .read_to_end(&mut read)
        .unwrap();
    assert_eq!(read, bytes);
    let state = server.state.lock().unwrap();
    let (_, key, headers, body) = state
        .requests
        .iter()
        .find(|(method, _, _, _)| method == "PUT")
        .unwrap();
    assert_eq!(key, "reader-a/ranges/first.hseg");
    assert_eq!(body, bytes);
    assert_eq!(
        headers["x-amz-content-sha256"],
        hex::encode(request.encoded_sha256)
    );
    assert!(headers.contains_key("content-md5"));
}

#[test]
fn lost_reply_recovers_without_a_second_write() {
    let server = Server::start();
    server.state.lock().unwrap().drop_put_response = true;
    let request = request(b"payload");
    assert!(
        store(&server)
            .create_if_absent(&request, &mut &b"payload"[..])
            .is_err()
    );
    // Construct a new backend, as on process restart.
    let recovered = store(&server)
        .create_if_absent(&request, &mut io::empty())
        .unwrap();
    assert_eq!(recovered.disposition, CreateDisposition::AlreadyExisted);
    assert_eq!(server.count("PUT"), 1);
}

#[test]
fn automatic_http_retry_resends_the_complete_shared_body() {
    let server = Server::start();
    server.state.lock().unwrap().drop_put_response = true;
    let store = R2ObjectStore::new(
        S3Client::new(server.settings(), 1).unwrap(),
        "reader-a".into(),
        1024 * 1024,
    )
    .unwrap();
    let bytes = (0..16_384u32)
        .flat_map(u32::to_be_bytes)
        .collect::<Vec<_>>();
    let result = store
        .create_if_absent(&request(&bytes), &mut &bytes[..])
        .unwrap();
    assert_eq!(result.disposition, CreateDisposition::AlreadyExisted);
    let state = server.state.lock().unwrap();
    let puts = state
        .requests
        .iter()
        .filter(|(method, _, _, _)| method == "PUT")
        .collect::<Vec<_>>();
    assert_eq!(puts.len(), 2);
    assert!(puts.iter().all(|(_, _, _, body)| *body == bytes));
    assert_eq!(state.objects.len(), 1);
}

#[test]
fn collisions_and_races_never_replace_existing_objects() {
    for concurrent in [false, true] {
        let server = Server::start();
        if concurrent {
            server.state.lock().unwrap().concurrent_put = Some(b"wrong!!".to_vec());
        } else {
            server
                .state
                .lock()
                .unwrap()
                .objects
                .insert("reader-a/ranges/first.hseg".into(), b"wrong!!".to_vec());
        }
        let request = request(b"payload");
        assert!(
            store(&server)
                .create_if_absent(&request, &mut &b"payload"[..])
                .is_err()
        );
        assert_eq!(
            server.state.lock().unwrap().objects["reader-a/ranges/first.hseg"],
            b"wrong!!"
        );
    }
}

#[test]
fn identical_concurrent_upload_is_an_exact_retry() {
    let server = Server::start();
    server.state.lock().unwrap().concurrent_put = Some(b"payload".to_vec());
    let result = store(&server)
        .create_if_absent(&request(b"payload"), &mut &b"payload"[..])
        .unwrap();
    assert_eq!(result.disposition, CreateDisposition::AlreadyExisted);
}

#[test]
fn invalid_source_and_remote_data_are_rejected() {
    let server = Server::start();
    let store = store(&server);
    let request = request(b"payload");
    for source in [&b"bad"[..], &b"too long"[..], &b"wrong!!"[..]] {
        assert!(store.create_if_absent(&request, &mut &source[..]).is_err());
    }
    assert_eq!(server.count("PUT"), 0);
    store
        .create_if_absent(&request, &mut &b"payload"[..])
        .unwrap();
    server.state.lock().unwrap().corrupt_get = true;
    assert!(matches!(
        store.verify_exact(&request),
        Err(Error::DigestMismatch { .. })
    ));
    server.state.lock().unwrap().corrupt_get = false;
    server.state.lock().unwrap().short_get = true;
    assert!(store.verify_exact(&request).is_err());
    server.state.lock().unwrap().short_get = false;
    server.state.lock().unwrap().encoded_get = true;
    assert!(store.open(&request.key).is_err());
    server.state.lock().unwrap().encoded_get = false;
    server.state.lock().unwrap().large_head = true;
    assert!(matches!(
        store.stat(&request.key),
        Err(Error::CapacityExceeded(_))
    ));
}

#[test]
fn failures_are_sanitized_and_retention_operations_are_unavailable() {
    let server = Server::start();
    let store = store(&server);
    server.state.lock().unwrap().fail_put = true;
    let request = request(b"payload");
    let error = store
        .create_if_absent(&request, &mut &b"payload"[..])
        .unwrap_err()
        .to_string();
    assert!(error.contains("503"));
    for secret in ["secret-response", "test-secret", server.endpoint.as_str()] {
        assert!(!error.contains(secret));
    }
    let before = server.state.lock().unwrap().requests.len();
    assert!(store.list("").is_err());
    assert!(store.delete(&request.key, None).is_err());
    assert_eq!(server.state.lock().unwrap().requests.len(), before);
    assert!(matches!(store.open(&request.key), Err(Error::NotFound(_))));
}

#[test]
fn namespace_binds_endpoint_bucket_prefix_but_allows_credential_rotation() {
    let server = Server::start();
    let original = store(&server).namespace_sha256();
    let mut settings = server.settings();
    settings.secret_key = "rotated-secret".into();
    let rotated = R2ObjectStore::new(
        S3Client::new(settings.clone(), 0).unwrap(),
        "reader-a".into(),
        100,
    )
    .unwrap();
    assert_eq!(original, rotated.namespace_sha256());
    settings.bucket = "other-bucket".into();
    let other =
        R2ObjectStore::new(S3Client::new(settings, 0).unwrap(), "reader-a".into(), 100).unwrap();
    assert_ne!(original, other.namespace_sha256());
    let other = R2ObjectStore::new(
        S3Client::new(server.settings(), 0).unwrap(),
        "reader-b".into(),
        100,
    )
    .unwrap();
    assert_ne!(original, other.namespace_sha256());
    let mut settings = server.settings();
    settings.endpoint = "http://127.0.0.1:1".into();
    let other =
        R2ObjectStore::new(S3Client::new(settings, 0).unwrap(), "reader-a".into(), 100).unwrap();
    assert_ne!(original, other.namespace_sha256());
    for prefix in ["", "/absolute", "reader/../other", "reader/", "reader\n"] {
        assert!(
            R2ObjectStore::new(
                S3Client::new(server.settings(), 0).unwrap(),
                prefix.into(),
                100
            )
            .is_err()
        );
    }
    assert_eq!(server.state.lock().unwrap().requests.len(), 0);
}

#[test]
fn configured_size_limit_prevents_remote_requests() {
    let server = Server::start();
    let store = R2ObjectStore::new(
        S3Client::new(server.settings(), 0).unwrap(),
        "reader-a".into(),
        1,
    )
    .unwrap();
    assert!(
        store
            .create_if_absent(&request(b"payload"), &mut &b"payload"[..])
            .is_err()
    );
    assert_eq!(server.state.lock().unwrap().requests.len(), 0);
}
