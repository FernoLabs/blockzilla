//! Ed25519 signing and verification for durable primary receipts.
//!
//! Key files are opened defensively and their contents are never included in errors or debug
//! output. Private keys must be readable only by their owner; public-key files may be shared but
//! must not be writable by group or other users.

use std::{
    collections::HashMap,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Take},
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use anyhow::{Context, Result, anyhow, ensure};
use ed25519_dalek::{
    Signature, Signer, SigningKey, VerifyingKey,
    pkcs8::{DecodePrivateKey, DecodePublicKey},
};
use zeroize::Zeroize;

use super::replication::{
    CumulativeAckSignatureVerifier, CumulativePrimaryAck, PrimaryReceipt, ReceiptSignatureVerifier,
};

/// Deliberately generous compared with normal PKCS#8/SPKI PEM encodings, while still bounding
/// memory use and accidental reads of unrelated files.
const MAX_KEY_FILE_BYTES: u64 = 16 * 1024;

/// Signs primary durability receipts using one configured PKCS#8 Ed25519 private key.
pub struct Ed25519ReceiptSigner {
    key_id: String,
    signing_key: SigningKey,
}

impl Ed25519ReceiptSigner {
    /// Load a PKCS#8 PEM private key from a regular, owner-only file.
    pub fn load_pkcs8_pem(key_id: impl Into<String>, path: impl AsRef<Path>) -> Result<Self> {
        let key_id = key_id.into();
        validate_key_id(&key_id)?;
        let path = path.as_ref();
        let mut encoded = read_key_file(path, KeyFileKind::Private)?;

        let signing_key = parse_private_key(&encoded)
            .with_context(|| format!("decode Ed25519 private key {}", path.display()));
        // The dalek key is zeroized on drop; also erase our temporary PEM representation as soon
        // as parsing has completed.
        encoded.zeroize();

        Ok(Self {
            key_id,
            signing_key: signing_key?,
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Sign all canonical receipt fields and atomically update the key id and signature fields.
    ///
    /// The caller's receipt is left unchanged if a field cannot be represented by the signing
    /// protocol.
    pub fn sign_receipt(&self, receipt: &mut PrimaryReceipt) -> Result<()> {
        let mut signed = receipt.clone();
        signed.signing_key_id.clone_from(&self.key_id);
        signed.signature.clear();
        let signing_bytes = signed
            .signing_bytes()
            .map_err(|error| anyhow!("receipt cannot be signed: {error:?}"))?;
        signed.signature = self.signing_key.sign(&signing_bytes).to_bytes().to_vec();
        *receipt = signed;
        Ok(())
    }

    /// Sign a cumulative durable-prefix ACK without mutating the caller's value on failure.
    pub fn sign_cumulative_ack(&self, ack: &mut CumulativePrimaryAck) -> Result<()> {
        let mut signed = ack.clone();
        signed.signing_key_id.clone_from(&self.key_id);
        signed.signature.clear();
        let signing_bytes = signed
            .signing_bytes()
            .map_err(|error| anyhow!("cumulative ACK cannot be signed: {error:?}"))?;
        signed.signature = self.signing_key.sign(&signing_bytes).to_bytes().to_vec();
        *ack = signed;
        Ok(())
    }
}

impl fmt::Debug for Ed25519ReceiptSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Ed25519ReceiptSigner")
            .field("key_id", &self.key_id)
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

/// Trusted Ed25519 receipt-verification keys, indexed by immutable key id.
#[derive(Default)]
pub struct Ed25519ReceiptKeyring {
    keys: HashMap<String, VerifyingKey>,
}

impl Ed25519ReceiptKeyring {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load one SPKI PEM public key. Reusing an existing id is rejected so rotation cannot
    /// silently replace a trust anchor.
    pub fn insert_spki_pem(
        &mut self,
        key_id: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let key_id = key_id.into();
        validate_key_id(&key_id)?;
        ensure!(
            !self.keys.contains_key(&key_id),
            "duplicate receipt verification key id {key_id:?}"
        );

        let path = path.as_ref();
        let encoded = read_key_file(path, KeyFileKind::Public)?;
        let pem = std::str::from_utf8(&encoded)
            .with_context(|| format!("public key is not UTF-8 PEM: {}", path.display()))?;
        let verifying_key = VerifyingKey::from_public_key_pem(pem)
            .with_context(|| format!("decode Ed25519 public key {}", path.display()))?;
        self.keys.insert(key_id, verifying_key);
        Ok(())
    }

    /// Convenience constructor for a keyring containing one SPKI PEM public key.
    pub fn load_spki_pem(key_id: impl Into<String>, path: impl AsRef<Path>) -> Result<Self> {
        let mut keyring = Self::new();
        keyring.insert_spki_pem(key_id, path)?;
        Ok(keyring)
    }

    pub fn contains_key(&self, key_id: &str) -> bool {
        self.keys.contains_key(key_id)
    }

    pub fn verify_signature(&self, key_id: &str, signing_bytes: &[u8], signature: &[u8]) -> bool {
        let Some(verifying_key) = self.keys.get(key_id) else {
            return false;
        };
        let Ok(signature) = Signature::try_from(signature) else {
            return false;
        };
        verifying_key
            .verify_strict(signing_bytes, &signature)
            .is_ok()
    }
}

impl fmt::Debug for Ed25519ReceiptKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut key_ids = self.keys.keys().collect::<Vec<_>>();
        key_ids.sort_unstable();
        formatter
            .debug_struct("Ed25519ReceiptKeyring")
            .field("key_ids", &key_ids)
            .finish()
    }
}

impl ReceiptSignatureVerifier for Ed25519ReceiptKeyring {
    fn verify_signature(&self, key_id: &str, signing_bytes: &[u8], signature: &[u8]) -> bool {
        Self::verify_signature(self, key_id, signing_bytes, signature)
    }
}

impl CumulativeAckSignatureVerifier for Ed25519ReceiptKeyring {
    fn verify_cumulative_ack_signature(
        &self,
        key_id: &str,
        signing_bytes: &[u8],
        signature: &[u8],
    ) -> bool {
        Self::verify_signature(self, key_id, signing_bytes, signature)
    }
}

fn validate_key_id(key_id: &str) -> Result<()> {
    ensure!(
        !key_id.is_empty() && key_id.len() <= 128 && !key_id.chars().any(char::is_control),
        "receipt key id must be 1..=128 bytes and contain no control characters"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum KeyFileKind {
    Private,
    Public,
}

impl KeyFileKind {
    fn label(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }
}

fn read_key_file(path: &Path, kind: KeyFileKind) -> Result<Vec<u8>> {
    let linked_before = fs::symlink_metadata(path)
        .with_context(|| format!("inspect {} key file {}", kind.label(), path.display()))?;
    ensure!(
        linked_before.file_type().is_file() && !linked_before.file_type().is_symlink(),
        "{} key path is not a regular non-symlink file: {}",
        kind.label(),
        path.display()
    );
    validate_key_metadata(path, kind, &linked_before)?;

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut file = options
        .open(path)
        .with_context(|| format!("open {} key file {}", kind.label(), path.display()))?;
    let opened_before = file.metadata().with_context(|| {
        format!(
            "inspect opened {} key file {}",
            kind.label(),
            path.display()
        )
    })?;
    validate_key_metadata(path, kind, &opened_before)?;
    validate_key_identity(path, kind, &opened_before, &linked_before)?;

    let mut encoded = Vec::with_capacity(opened_before.len() as usize);
    let mut limited: Take<&mut File> = file.by_ref().take(MAX_KEY_FILE_BYTES + 1);
    limited
        .read_to_end(&mut encoded)
        .with_context(|| format!("read {} key file {}", kind.label(), path.display()))?;
    ensure!(
        encoded.len() as u64 == opened_before.len(),
        "{} key file changed while being read: {}",
        kind.label(),
        path.display()
    );

    // Opening with O_NOFOLLOW pins the descriptor to one inode. Rechecking both the descriptor
    // and the operator-visible path also rejects replacement, truncation, unsafe chmod, and chown
    // races instead of accepting a key whose configured pathname changed during validation.
    let opened_after = file.metadata().with_context(|| {
        format!(
            "reinspect opened {} key file {}",
            kind.label(),
            path.display()
        )
    })?;
    let linked_after = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect {} key file {}", kind.label(), path.display()))?;
    validate_key_metadata(path, kind, &opened_after)?;
    validate_key_metadata(path, kind, &linked_after)?;
    validate_key_identity(path, kind, &opened_after, &linked_after)?;
    ensure!(
        opened_after.len() == opened_before.len(),
        "{} key file changed while being read: {}",
        kind.label(),
        path.display()
    );
    Ok(encoded)
}

fn validate_key_identity(
    path: &Path,
    kind: KeyFileKind,
    opened: &std::fs::Metadata,
    linked: &std::fs::Metadata,
) -> Result<()> {
    ensure!(
        linked.file_type().is_file() && !linked.file_type().is_symlink(),
        "{} key path is not a regular non-symlink file: {}",
        kind.label(),
        path.display()
    );
    #[cfg(unix)]
    ensure!(
        opened.dev() == linked.dev() && opened.ino() == linked.ino(),
        "{} key file changed while it was opened: {}",
        kind.label(),
        path.display()
    );
    #[cfg(not(unix))]
    let _ = opened;
    Ok(())
}

fn validate_key_metadata(
    path: &Path,
    kind: KeyFileKind,
    metadata: &std::fs::Metadata,
) -> Result<()> {
    ensure!(
        metadata.is_file(),
        "{} key path is not a regular file: {}",
        kind.label(),
        path.display()
    );
    ensure!(
        metadata.len() > 0 && metadata.len() <= MAX_KEY_FILE_BYTES,
        "{} key file must be between 1 byte and {} bytes: {}",
        kind.label(),
        MAX_KEY_FILE_BYTES,
        path.display()
    );

    #[cfg(unix)]
    {
        let mode = metadata.permissions().mode() & 0o777;
        match kind {
            KeyFileKind::Private => ensure!(
                mode & 0o177 == 0 && mode & 0o400 != 0,
                "private key file permissions must be 0400 or 0600, found {mode:04o}: {}",
                path.display()
            ),
            KeyFileKind::Public => ensure!(
                mode & 0o133 == 0,
                "public key file must not be executable or writable by group/others, found {mode:04o}: {}",
                path.display()
            ),
        }
        let effective_uid = unsafe { libc::geteuid() };
        ensure!(
            trusted_key_owner(metadata.uid(), effective_uid),
            "{} key file has an untrusted owner: {}",
            kind.label(),
            path.display()
        );
    }
    Ok(())
}

#[cfg(unix)]
fn trusted_key_owner(owner: u32, effective_uid: u32) -> bool {
    owner == effective_uid || owner == 0
}

fn parse_private_key(encoded: &[u8]) -> Result<SigningKey> {
    let pem = std::str::from_utf8(encoded).context("private key is not UTF-8 PEM")?;
    SigningKey::from_pkcs8_pem(pem).context("invalid PKCS#8 Ed25519 private key")
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    use ed25519_dalek::{
        SigningKey,
        pkcs8::{EncodePrivateKey, EncodePublicKey, spki::der::pem::LineEnding},
    };

    use super::*;
    use crate::ingest::{
        ContentDigest, CumulativePrimaryAck, ExpectedCumulativeAck, ExpectedReceipt, ObservationId,
        REPLICATION_PROTOCOL_VERSION, ReceiptDisposition, ReceiptValidationError,
        ReplicationStreamId, verify_cumulative_ack, verify_receipt,
    };

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "blockzilla-receipt-crypto-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("create test directory");
            Self(path)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_file(path: &Path, contents: &[u8], mode: u32) {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create key file");
        file.write_all(contents).expect("write key file");
        file.sync_all().expect("sync key file");
        #[cfg(unix)]
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("set key permissions");
        #[cfg(not(unix))]
        let _ = mode;
    }

    fn write_key_pair(directory: &TestDirectory, seed: u8, stem: &str) -> (PathBuf, PathBuf) {
        let signing_key = SigningKey::from_bytes(&[seed; 32]);
        let private_pem = signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("encode private key");
        let public_pem = signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("encode public key");
        let private_path = directory.path(&format!("{stem}.private.pem"));
        let public_path = directory.path(&format!("{stem}.public.pem"));
        write_file(&private_path, private_pem.as_bytes(), 0o600);
        write_file(&public_path, public_pem.as_bytes(), 0o644);
        (private_path, public_path)
    }

    fn receipt() -> PrimaryReceipt {
        PrimaryReceipt {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: "solana-mainnet".to_owned(),
            primary_id: "blockzilla-primary".to_owned(),
            primary_term: 7,
            record: ObservationId {
                origin_node_id: "hetzner-replica".to_owned(),
                journal_id: [3; 16],
                sequence: 42,
            },
            content_digest: ContentDigest([9; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 1002,
            archive_commit_digest: None,
            signing_key_id: "placeholder".to_owned(),
            signature: Vec::new(),
        }
    }

    fn cumulative_ack() -> CumulativePrimaryAck {
        CumulativePrimaryAck {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            stream: ReplicationStreamId {
                cluster_id: "solana-mainnet".to_owned(),
                origin_node_id: "hetzner-replica".to_owned(),
                source_id: "grpc-raw".to_owned(),
                journal_id: [3; 16],
            },
            primary_id: "blockzilla-primary".to_owned(),
            primary_term: 7,
            through_sequence: 42,
            through_content_digest: ContentDigest([9; 32]),
            rolling_chain_digest: ContentDigest([10; 32]),
            disposition: ReceiptDisposition::DurablyStored,
            durable_lsn: 1002,
            signing_key_id: "placeholder".to_owned(),
            signature: Vec::new(),
        }
    }

    #[test]
    fn signs_and_verifies_a_receipt_with_loaded_pem_keys() {
        let directory = TestDirectory::create("round-trip");
        let (private_path, public_path) = write_key_pair(&directory, 7, "active");
        let signer = Ed25519ReceiptSigner::load_pkcs8_pem("receipt-2026-07", private_path)
            .expect("load signer");
        let keyring = Ed25519ReceiptKeyring::load_spki_pem("receipt-2026-07", public_path)
            .expect("load keyring");
        let mut receipt = receipt();

        signer.sign_receipt(&mut receipt).expect("sign receipt");

        assert_eq!(receipt.signing_key_id, "receipt-2026-07");
        assert_eq!(receipt.signature.len(), 64);
        assert!(keyring.verify_signature(
            &receipt.signing_key_id,
            &receipt.signing_bytes().expect("canonical signing bytes"),
            &receipt.signature,
        ));
        let expected = ExpectedReceipt {
            cluster_id: &receipt.cluster_id,
            primary_id: &receipt.primary_id,
            record: &receipt.record,
            content_digest: receipt.content_digest,
        };
        assert!(verify_receipt(receipt.clone(), expected, &keyring).is_ok());
        assert!(format!("{signer:?}").contains("<redacted>"));
        assert!(!format!("{signer:?}").contains("PRIVATE KEY"));
    }

    #[test]
    fn signs_and_verifies_a_cumulative_ack_with_the_same_keyring() {
        let directory = TestDirectory::create("cumulative-round-trip");
        let (private_path, public_path) = write_key_pair(&directory, 8, "active");
        let signer = Ed25519ReceiptSigner::load_pkcs8_pem("receipt-2026-07", private_path)
            .expect("load signer");
        let keyring = Ed25519ReceiptKeyring::load_spki_pem("receipt-2026-07", public_path)
            .expect("load keyring");
        let mut ack = cumulative_ack();

        signer
            .sign_cumulative_ack(&mut ack)
            .expect("sign cumulative ACK");

        let expected = ExpectedCumulativeAck {
            stream: &ack.stream,
            primary_id: &ack.primary_id,
            minimum_primary_term: ack.primary_term,
            through_sequence: ack.through_sequence,
            through_content_digest: ack.through_content_digest,
            rolling_chain_digest: ack.rolling_chain_digest,
        };
        assert!(verify_cumulative_ack(ack.clone(), expected, &keyring).is_ok());
    }

    #[test]
    fn rejects_a_signature_made_by_a_different_key() {
        let directory = TestDirectory::create("wrong-key");
        let (private_path, _) = write_key_pair(&directory, 11, "signer");
        let (_, wrong_public_path) = write_key_pair(&directory, 12, "verifier");
        let signer =
            Ed25519ReceiptSigner::load_pkcs8_pem("active", private_path).expect("load signer");
        let keyring = Ed25519ReceiptKeyring::load_spki_pem("active", wrong_public_path)
            .expect("load wrong public key");
        let mut receipt = receipt();
        signer.sign_receipt(&mut receipt).expect("sign receipt");
        let expected = ExpectedReceipt {
            cluster_id: &receipt.cluster_id,
            primary_id: &receipt.primary_id,
            record: &receipt.record,
            content_digest: receipt.content_digest,
        };

        assert_eq!(
            verify_receipt(receipt.clone(), expected, &keyring),
            Err(ReceiptValidationError::InvalidSignature)
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symbolic_link_key_files() {
        let directory = TestDirectory::create("symlink");
        let (private_path, public_path) = write_key_pair(&directory, 21, "target");
        let private_link = directory.path("private-link.pem");
        let public_link = directory.path("public-link.pem");
        symlink(&private_path, &private_link).expect("link private key");
        symlink(&public_path, &public_link).expect("link public key");

        assert!(Ed25519ReceiptSigner::load_pkcs8_pem("active", private_link).is_err());
        assert!(Ed25519ReceiptKeyring::load_spki_pem("active", public_link).is_err());
    }

    #[test]
    fn rejects_oversized_key_files_before_decoding() {
        let directory = TestDirectory::create("oversize");
        let private_path = directory.path("large-private.pem");
        let public_path = directory.path("large-public.pem");
        let oversized = vec![b'x'; MAX_KEY_FILE_BYTES as usize + 1];
        write_file(&private_path, &oversized, 0o600);
        write_file(&public_path, &oversized, 0o644);

        assert!(Ed25519ReceiptSigner::load_pkcs8_pem("active", private_path).is_err());
        assert!(Ed25519ReceiptKeyring::load_spki_pem("active", public_path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_permissive_private_key_permissions() {
        let directory = TestDirectory::create("permissions");
        let (private_path, _) = write_key_pair(&directory, 31, "permissive");
        fs::set_permissions(&private_path, fs::Permissions::from_mode(0o644))
            .expect("make private key permissive");

        let error = Ed25519ReceiptSigner::load_pkcs8_pem("active", private_path)
            .expect_err("permissive private key must fail");
        assert!(error.to_string().contains("0400 or 0600"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_permissive_public_key_permissions() {
        let directory = TestDirectory::create("public-permissions");
        let (_, public_path) = write_key_pair(&directory, 32, "permissive");
        fs::set_permissions(&public_path, fs::Permissions::from_mode(0o666))
            .expect("make public key writable by everyone");

        let error = Ed25519ReceiptKeyring::load_spki_pem("active", public_path)
            .expect_err("writable public key must fail");
        assert!(error.to_string().contains("writable by group/others"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_an_inode_replaced_between_path_inspection_and_open() {
        let directory = TestDirectory::create("inode-replacement");
        let first = directory.path("first.pem");
        let replacement = directory.path("replacement.pem");
        write_file(&first, b"first", 0o600);
        write_file(&replacement, b"second", 0o600);

        let opened = fs::metadata(&first).expect("inspect opened fixture");
        let linked = fs::symlink_metadata(&replacement).expect("inspect replacement fixture");
        let error = validate_key_identity(&first, KeyFileKind::Private, &opened, &linked)
            .expect_err("different inode must fail");
        assert!(error.to_string().contains("changed while it was opened"));
    }

    #[cfg(unix)]
    #[test]
    fn key_owner_policy_accepts_only_effective_user_or_root() {
        assert!(trusted_key_owner(1_000, 1_000));
        assert!(trusted_key_owner(0, 1_000));
        assert!(!trusted_key_owner(1_001, 1_000));
    }
}
