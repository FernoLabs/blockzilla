//! Bounded immutable R2 objects through the existing signed S3 client.
//!
//! R2 ETags and user-supplied SHA metadata are not provider-attested SHA-256.
//! `verify_exact` therefore performs a full streamed read-back. R2 does not
//! provide an object version here. Listing and deletion are deliberately unavailable;
//! callers recover from their durable range catalog and retain source data on error.
//! This backend performs blocking I/O; use a blocking worker in async runtimes.

use std::{io::Read, sync::Arc};

use base64::Engine;
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::{
    CreateDisposition, CreateRequest, CreateResult, DeleteDisposition, Error, ImmutableObjectStore,
    ObjectKey, ObjectMetadata, ObjectRead, ObjectVersion, Result, read_and_verify,
    uploader::{Payload, Provider, S3Client, S3Response, UploaderError},
    verify_metadata,
};

pub const MAX_R2_OBJECT_BYTES: u64 = 256 * 1024 * 1024;

pub struct R2ObjectStore {
    client: S3Client,
    prefix: String,
    max_object_bytes: u64,
    namespace_sha256: [u8; 32],
}

impl R2ObjectStore {
    pub fn new(client: S3Client, prefix: String, max_object_bytes: u64) -> Result<Self> {
        if client.provider != Provider::R2 {
            return Err(Error::UnsupportedOperation(
                "R2 backend requires an R2 client",
            ));
        }
        ObjectKey::new(&prefix)?;
        if prefix.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(Error::InvalidKey("R2 prefix contains a control character"));
        }
        if max_object_bytes == 0 || max_object_bytes > MAX_R2_OBJECT_BYTES {
            return Err(Error::CapacityExceeded(max_object_bytes));
        }
        // Length framing prevents ambiguous namespace identities. Credentials can rotate
        // without changing identity; a different endpoint, bucket, or prefix cannot.
        let mut digest = Sha256::new();
        digest.update(b"hivezilla-r2-namespace-v1\0");
        for field in [client.endpoint(), &client.bucket, &prefix] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        Ok(Self {
            client,
            prefix,
            max_object_bytes,
            namespace_sha256: digest.finalize().into(),
        })
    }

    pub fn namespace_sha256(&self) -> [u8; 32] {
        self.namespace_sha256
    }

    fn remote_key(&self, key: &ObjectKey) -> Result<String> {
        let remote = ObjectKey::new(format!("{}/{}", self.prefix, key.as_str()))?;
        if remote.as_str().bytes().any(|byte| byte.is_ascii_control()) {
            return Err(Error::InvalidKey("R2 key contains a control character"));
        }
        Ok(remote.as_str().to_owned())
    }

    fn check_length(&self, length: u64) -> Result<()> {
        if length > self.max_object_bytes {
            return Err(Error::CapacityExceeded(length));
        }
        Ok(())
    }

    fn metadata(&self, key: &ObjectKey, response: &S3Response) -> Result<ObjectMetadata> {
        if response.status != 200 || response.headers.contains_key("content-range") {
            return Err(invalid_response("whole-object response", response));
        }
        if response.headers.contains_key("content-encoding")
            && response
                .exact_header("content-encoding", "R2 metadata")
                .map_err(|error| backend_error("metadata", error, key))?
                != "identity"
        {
            return Err(invalid_response("encoded object response", response));
        }
        let encoded_len = response
            .exact_content_length("R2 metadata")
            .map_err(|error| backend_error("metadata", error, key))?;
        self.check_length(encoded_len)?;
        Ok(ObjectMetadata {
            key: key.clone(),
            encoded_len,
            provider_sha256: None,
            version: None,
        })
    }
}

impl ImmutableObjectStore for R2ObjectStore {
    fn create_if_absent(
        &self,
        request: &CreateRequest,
        source: &mut dyn Read,
    ) -> Result<CreateResult> {
        self.check_length(request.encoded_len)?;
        let remote_key = self.remote_key(&request.key)?;
        if self.stat(&request.key)?.is_some() {
            // Exact retries work without reading source, including a lost PUT response.
            return Ok(CreateResult {
                disposition: CreateDisposition::AlreadyExisted,
                metadata: self.verify_exact(request)?,
            });
        }
        let bytes = read_and_verify(source, request.encoded_len, request.encoded_sha256)?;
        let md5 = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&bytes));
        let payload = Payload::SharedBytes(Arc::from(bytes));
        let response = self
            .client
            .put(
                &remote_key,
                "application/octet-stream",
                &hex::encode(request.encoded_sha256),
                Some(&md5),
                true,
                &payload,
            )
            .map_err(|error| backend_error("upload", error, &request.key))?;
        let disposition = match response.status {
            200 => CreateDisposition::Created,
            412 => CreateDisposition::AlreadyExisted,
            _ => return Err(invalid_response("upload response", &response)),
        };
        drop(response);
        let metadata = if disposition == CreateDisposition::AlreadyExisted {
            self.verify_exact(request)?
        } else {
            // Content-MD5 and the signed body digest protect the upload. This metadata
            // is not SHA-256 attestation: the catalog still calls verify_exact before
            // committing its receipt, which streams and hashes the remote object.
            let metadata = self
                .stat(&request.key)?
                .ok_or_else(|| Error::NotFound(request.key.clone()))?;
            verify_metadata(request, &metadata)?;
            metadata
        };
        Ok(CreateResult {
            disposition,
            metadata,
        })
    }

    fn open(&self, key: &ObjectKey) -> Result<ObjectRead> {
        let response = self
            .client
            .get(&self.remote_key(key)?)
            .map_err(|error| backend_error("download", error, key))?;
        let metadata = self.metadata(key, &response)?;
        Ok(ObjectRead {
            metadata,
            body: Box::new(response.into_reader()),
        })
    }

    fn stat(&self, key: &ObjectKey) -> Result<Option<ObjectMetadata>> {
        let response = self
            .client
            .head(&self.remote_key(key)?)
            .map_err(|error| backend_error("inspect", error, key))?;
        if response.status == 404 {
            return Ok(None);
        }
        self.metadata(key, &response).map(Some)
    }

    fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>> {
        Err(Error::UnsupportedOperation(
            "R2 listing; use the durable range catalog",
        ))
    }

    fn delete(
        &self,
        _key: &ObjectKey,
        _expected_version: Option<&ObjectVersion>,
    ) -> Result<DeleteDisposition> {
        Err(Error::UnsupportedOperation(
            "R2 deletion; retention is not enabled",
        ))
    }
}

fn invalid_response(operation: &'static str, response: &S3Response) -> Error {
    Error::Backend {
        operation,
        status: Some(response.status),
    }
}

fn backend_error(operation: &'static str, error: UploaderError, key: &ObjectKey) -> Error {
    let status = match error {
        UploaderError::Api(error) => Some(error.status),
        _ => None,
    };
    if status == Some(404) {
        Error::NotFound(key.clone())
    } else {
        // Do not copy remote error text, credentials, or endpoint URLs into logs.
        Error::Backend { operation, status }
    }
}
