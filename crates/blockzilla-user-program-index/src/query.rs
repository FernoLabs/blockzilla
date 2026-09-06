//! Bound, random-access lookup for the per-epoch signer user -> reached-program index.

use std::{
    fs::{self, File},
    io::Read,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};

use anyhow::{Context, Result};
use blockzilla_archive_v2::{ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE};
use blockzilla_registry::FileBackedKeyIndex;
use blockzilla_compact_v2_reader::manifest::{GENERATION_MANIFEST_FILE, GenerationManifest};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::format::{
    GenerationBindingKind, IndexFileBinding, IndexManifest, IndexReader, ProgramMapReader,
    RegistryFileIdentity, open_file,
};

const MAX_GENERATION_MANIFEST_BYTES: u64 = 4 << 20;

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub wallet: String,
    pub epoch: u64,
    pub index_wallet_count: u64,
    pub index_program_count: u64,
    pub programs: Vec<String>,
}

/// Public query shape for user-program index commands.
///
/// The older [`QueryResult`] keeps its Rust `wallet` names for source
/// compatibility. This type gives new CLI JSON the generic `user` and
/// `index_user_count` field names.
#[derive(Debug, Serialize)]
pub struct UserProgramQueryResult {
    pub user: String,
    pub epoch: u64,
    pub index_user_count: u64,
    pub index_program_count: u64,
    pub programs: Vec<String>,
}

impl From<QueryResult> for UserProgramQueryResult {
    fn from(value: QueryResult) -> Self {
        Self {
            user: value.wallet,
            epoch: value.epoch,
            index_user_count: value.index_wallet_count,
            index_program_count: value.index_program_count,
            programs: value.programs,
        }
    }
}

/// Query one signer user and return generic user-program JSON field names.
pub fn query_user_program_index(
    index_dir: &Path,
    archive_root: &Path,
    user: &str,
    trust_local: bool,
) -> Result<UserProgramQueryResult> {
    query_index(index_dir, archive_root, user, trust_local).map(Into::into)
}

/// Query a published-manifest-bound index. `trust_local` is an explicit
/// acknowledgement required only for an index built with `--trust-local`; in
/// that mode the exact registry file identity and requested wallet bytes are
/// checked, but the synthetic archive generation identity remains an assertion.
pub fn query_index(
    index_dir: &Path,
    archive_root: &Path,
    wallet: &str,
    trust_local: bool,
) -> Result<QueryResult> {
    let manifest = IndexManifest::read(index_dir)
        .with_context(|| format!("read manifest at {}", index_dir.display()))?;
    let canonical_archive_root = fs::canonicalize(archive_root)
        .with_context(|| format!("canonicalize archive root {}", archive_root.display()))?;
    let registry_path = canonical_archive_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let registry = open_file(&registry_path)
        .with_context(|| format!("open retained registry {}", registry_path.display()))?;
    let registry_identity = file_identity_for_file(&registry, &registry_path)?;
    anyhow::ensure!(
        registry_identity.size == manifest.registry.size,
        "registry.bin size does not match the index manifest binding"
    );
    let key_index_path = canonical_archive_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let key_index_file = open_file(&key_index_path)
        .with_context(|| format!("open retained registry index {}", key_index_path.display()))?;
    let key_index_identity = file_identity_for_file(&key_index_file, &key_index_path)?;
    anyhow::ensure!(
        key_index_identity.size == manifest.registry_index.size,
        "registry.mphf size does not match the index manifest binding"
    );

    let original_archive = canonical_archive_root == Path::new(&manifest.archive_root);

    match manifest.binding_kind {
        GenerationBindingKind::PublishedManifest => {
            anyhow::ensure!(
                !trust_local,
                "--trust-local is only valid for an index built in trusted-local mode"
            );
            verify_published_binding(&canonical_archive_root, &manifest)?;
            if original_archive {
                anyhow::ensure!(
                    registry_identity == manifest.registry_file_identity,
                    "published registry.bin file identity changed since the index was built"
                );
                anyhow::ensure!(
                    key_index_identity == manifest.registry_index_file_identity,
                    "published registry.mphf file identity changed since the index was built"
                );
            } else {
                verify_file_binding(&registry, &registry_path, &manifest.registry)
                    .context("verify relocated registry.bin content binding")?;
                verify_file_binding(&key_index_file, &key_index_path, &manifest.registry_index)
                    .context("verify relocated registry.mphf content binding")?;
            }
        }
        GenerationBindingKind::TrustedLocalAssertedImmutable => {
            anyhow::ensure!(
                trust_local,
                "this index was built from an asserted trusted-local archive; pass --trust-local to acknowledge that unverified generation identity"
            );
            anyhow::ensure!(
                original_archive,
                "trusted-local query archive {} is not the exact archive path used to build the index ({})",
                canonical_archive_root.display(),
                manifest.archive_root
            );
            anyhow::ensure!(
                registry_identity == manifest.registry_file_identity,
                "trusted-local registry.bin file identity changed since the index was built"
            );
            anyhow::ensure!(
                key_index_identity == manifest.registry_index_file_identity,
                "trusted-local registry.mphf file identity changed since the index was built"
            );
        }
    }

    let wallet_bytes = decode_wallet(wallet)?;
    let key_index = FileBackedKeyIndex::load_file(
        key_index_file
            .try_clone()
            .context("clone retained registry.mphf handle")?,
        &key_index_path,
    )
    .with_context(|| format!("open {}", key_index_path.display()))?;
    anyhow::ensure!(
        key_index.len() == manifest.registry_entries as usize,
        "registry.mphf contains {} keys, index manifest expects {}",
        key_index.len(),
        manifest.registry_entries
    );
    let wallet_id = key_index
        .lookup(&wallet_bytes)
        .context("look up wallet in registry.mphf")?
        .with_context(|| format!("{wallet} is not a known registry pubkey for this archive"))?;

    let resolved_wallet = pubkey_at(&registry, wallet_id, manifest.registry_entries)
        .with_context(|| format!("verify wallet registry id {wallet_id}"))?;
    anyhow::ensure!(
        resolved_wallet == wallet_bytes,
        "registry.mphf is stale or belongs to a different registry: id {wallet_id} does not resolve to {wallet}"
    );

    let shard_name = manifest.shard_dir_name(wallet_id)?;
    let shard_dir = index_dir.join(shard_name);
    let shard_binding = manifest.shard_binding(wallet_id)?;
    let reader = IndexReader::open_verified(&shard_dir, shard_binding)
        .with_context(|| format!("open shard at {}", shard_dir.display()))?;
    let program_ids = reader
        .query(wallet_id)
        .with_context(|| format!("query shard at {}", shard_dir.display()))?;
    let program_map =
        ProgramMapReader::open_verified(index_dir, &manifest.program_map, manifest.program_count)
            .with_context(|| format!("open bound program map at {}", index_dir.display()))?;

    let mut programs = Vec::with_capacity(program_ids.len());
    for id in program_ids {
        let pubkey = program_map
            .resolve(id)
            .with_context(|| format!("resolve bound program registry id {id}"))?;
        programs.push(bs58::encode(pubkey).into_string());
    }
    programs.sort_unstable();

    reader
        .verify_unchanged()
        .context("index shard changed during query")?;
    program_map
        .verify_unchanged()
        .context("programs.map changed during query")?;

    let result = QueryResult {
        wallet: bs58::encode(wallet_bytes).into_string(),
        epoch: manifest.epoch,
        index_wallet_count: manifest.wallet_count,
        index_program_count: manifest.program_count,
        programs,
    };

    ensure_file_unchanged(&registry, &registry_path, &registry_identity)
        .context("registry.bin changed during query")?;
    ensure_file_unchanged(&key_index_file, &key_index_path, &key_index_identity)
        .context("registry.mphf changed during query")?;

    Ok(result)
}

fn verify_published_binding(archive_root: &Path, index: &IndexManifest) -> Result<()> {
    let path = archive_root.join(GENERATION_MANIFEST_FILE);
    let file = open_file(&path).with_context(|| format!("open {}", path.display()))?;
    let initial_identity = file_identity_for_file(&file, &path)?;
    let size = initial_identity.size;
    anyhow::ensure!(
        size <= MAX_GENERATION_MANIFEST_BYTES,
        "{} is {size} bytes, above the {}-byte limit",
        path.display(),
        MAX_GENERATION_MANIFEST_BYTES
    );
    let capacity = usize::try_from(size).context("generation manifest size exceeds usize")?;
    let read_limit = MAX_GENERATION_MANIFEST_BYTES
        .checked_add(1)
        .context("generation manifest read limit overflow")?;
    let mut bytes = Vec::with_capacity(capacity);
    (&file)
        .take(read_limit)
        .read_to_end(&mut bytes)
        .with_context(|| format!("read retained {}", path.display()))?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_GENERATION_MANIFEST_BYTES,
        "{} grew beyond the {}-byte limit while it was being read",
        path.display(),
        MAX_GENERATION_MANIFEST_BYTES
    );
    anyhow::ensure!(
        file_identity_for_file(&file, &path)? == initial_identity,
        "{} changed while it was being read",
        path.display()
    );
    let generation =
        GenerationManifest::parse(&bytes).with_context(|| format!("parse {}", path.display()))?;
    anyhow::ensure!(generation.complete, "archive generation is not complete");
    anyhow::ensure!(
        generation.cluster_id == index.cluster_id
            && generation.epoch == index.epoch
            && generation.generation_id == index.generation_id
            && generation.generation_digest == index.generation_digest,
        "archive generation identity does not match the built index"
    );
    let registry = generation
        .file(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
        .context("archive generation manifest has no registry.bin entry")?;
    anyhow::ensure!(
        registry.sha256 == index.registry.sha256 && registry.size == index.registry.size,
        "archive registry binding does not match the built index"
    );
    Ok(())
}

fn decode_wallet(wallet: &str) -> Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    let written = bs58::decode(wallet)
        .onto(&mut bytes)
        .with_context(|| format!("invalid base58 wallet pubkey {wallet}"))?;
    anyhow::ensure!(
        written == 32,
        "wallet pubkey must decode to exactly 32 bytes"
    );
    Ok(bytes)
}

fn pubkey_at(file: &File, id: u32, registry_entries: u32) -> Result<[u8; 32]> {
    anyhow::ensure!(
        id != 0 && id <= registry_entries,
        "registry id {id} is outside 1..={registry_entries}"
    );
    let offset = u64::from(id - 1) * 32;
    let mut bytes = [0u8; 32];
    file.read_exact_at(&mut bytes, offset)
        .with_context(|| format!("read registry.bin at byte offset {offset}"))?;
    Ok(bytes)
}

fn file_identity(path: &Path) -> Result<RegistryFileIdentity> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    file_identity_from_metadata(metadata, path)
}

fn file_identity_for_file(file: &File, path: &Path) -> Result<RegistryFileIdentity> {
    let metadata = file
        .metadata()
        .with_context(|| format!("stat retained {}", path.display()))?;
    file_identity_from_metadata(metadata, path)
}

fn file_identity_from_metadata(
    metadata: fs::Metadata,
    path: &Path,
) -> Result<RegistryFileIdentity> {
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok(RegistryFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn sha256_file_handle(file: &File, path: &Path) -> Result<String> {
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    let mut hasher = Sha256::new();
    let expected_len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    let mut offset = 0u64;
    while offset < expected_len {
        let remaining = usize::try_from((expected_len - offset).min(buffer.len() as u64))
            .context("artifact hash chunk length exceeds usize")?;
        let read = file
            .read_at(&mut buffer[..remaining], offset)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            anyhow::bail!("{} was truncated while hashing", path.display());
        }
        hasher.update(&buffer[..read]);
        offset += read as u64;
    }
    anyhow::ensure!(
        file.metadata()
            .with_context(|| format!("re-stat {}", path.display()))?
            .len()
            == expected_len,
        "{} changed length while hashing",
        path.display()
    );
    let digest = hasher.finalize();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(encoded)
}

fn verify_file_binding(file: &File, path: &Path, binding: &IndexFileBinding) -> Result<()> {
    anyhow::ensure!(
        file.metadata()
            .with_context(|| format!("stat retained {}", path.display()))?
            .len()
            == binding.size,
        "{} size does not match the built index",
        path.display()
    );
    anyhow::ensure!(
        sha256_file_handle(file, path)? == binding.sha256,
        "{} SHA-256 does not match the built index",
        path.display()
    );
    Ok(())
}

fn ensure_file_unchanged(file: &File, path: &Path, initial: &RegistryFileIdentity) -> Result<()> {
    anyhow::ensure!(
        file_identity_for_file(file, path)? == *initial && file_identity(path)? == *initial,
        "retained file or path identity changed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use blockzilla_archive_v2::{ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE};
    use blockzilla_registry::{KeyIndex, write_registry};
    use blockzilla_compact_v2_reader::manifest::{
        GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile, compute_generation_digest,
    };

    use crate::format::{
        FORMAT_VERSION, IndexBuilder, IndexSemantics, MANIFEST_SCHEMA_VERSION, OmissionCounts,
        bind_shard, write_program_map,
    };

    struct QueryFixture {
        archive: PathBuf,
        index: PathBuf,
        wallet: [u8; 32],
        program: [u8; 32],
        registry_index_size: u64,
    }

    fn artifact_binding(path: &Path) -> (IndexFileBinding, RegistryFileIdentity) {
        let file = File::open(path).unwrap();
        let identity = file_identity_for_file(&file, path).unwrap();
        let binding = IndexFileBinding {
            size: identity.size,
            sha256: sha256_file_handle(&file, path).unwrap(),
        };
        (binding, identity)
    }

    fn write_registry_pair(archive: &Path, keys: &[[u8; 32]]) {
        fs::create_dir_all(archive).unwrap();
        write_registry(&archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE), keys).unwrap();
        KeyIndex::build_from_slice(keys)
            .write(&archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
    }

    fn write_generation_manifest(
        archive: &Path,
        registry: &IndexFileBinding,
    ) -> GenerationManifest {
        let mut generation = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: "testnet".into(),
            epoch: 1,
            generation_id: "fixture-generation".into(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: vec![GenerationFile {
                name: ARCHIVE_V2_PUBKEY_REGISTRY_FILE.into(),
                size: registry.size,
                sha256: registry.sha256.clone(),
            }],
        };
        generation.generation_digest = compute_generation_digest(&generation).unwrap();
        fs::write(
            archive.join(GENERATION_MANIFEST_FILE),
            serde_json::to_vec_pretty(&generation).unwrap(),
        )
        .unwrap();
        generation
    }

    fn query_fixture(root: &Path) -> QueryFixture {
        let archive = root.join("original-archive");
        let index = root.join("index");
        let wallet = [1u8; 32];
        let program = [2u8; 32];
        write_registry_pair(&archive, &[wallet, program]);

        let registry_path = archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let registry_index_path = archive.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let (registry, registry_file_identity) = artifact_binding(&registry_path);
        let (registry_index, registry_index_file_identity) = artifact_binding(&registry_index_path);
        let generation = write_generation_manifest(&archive, &registry);

        let shard = index.join("shard-0");
        let mut builder = IndexBuilder::new(1, 2, 2);
        builder.record(1, 2);
        builder.write(&shard).unwrap();
        let shards = vec![bind_shard(0, &shard, 2, 2).unwrap()];
        let program_map = write_program_map(&index, &[(2, program)]).unwrap();

        IndexManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            format_version: FORMAT_VERSION,
            semantics: IndexSemantics::current(),
            complete: true,
            omissions: OmissionCounts::default(),
            binding_kind: GenerationBindingKind::PublishedManifest,
            cluster_id: generation.cluster_id,
            epoch: generation.epoch,
            archive_root: fs::canonicalize(&archive).unwrap().display().to_string(),
            generation_id: generation.generation_id,
            generation_digest: generation.generation_digest,
            registry,
            registry_file_identity,
            registry_index: registry_index.clone(),
            registry_index_file_identity,
            registry_entries: 2,
            chunk_width: 2,
            shard_count: 1,
            shards,
            program_map,
            wallet_count: 1,
            program_count: 1,
            transactions_scanned: 1,
            blocks_scanned: 1,
            failed_transactions_excluded: 0,
            built_unix_time: 1,
            tool_version: "test".into(),
        }
        .write(&index)
        .unwrap();

        QueryFixture {
            archive,
            index,
            wallet,
            program,
            registry_index_size: registry_index.size,
        }
    }

    fn copy_fixture_index(source: &Path, destination: &Path) {
        fs::create_dir_all(destination.join("shard-0")).unwrap();
        for name in ["manifest.json", "programs.map"] {
            fs::copy(source.join(name), destination.join(name)).unwrap();
        }
        for name in ["wallets.idx", "programs.rel"] {
            fs::copy(
                source.join("shard-0").join(name),
                destination.join("shard-0").join(name),
            )
            .unwrap();
        }
    }

    #[test]
    fn positioned_registry_reads_are_one_based_and_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("registry.bin");
        let first = [1u8; 32];
        let second = [2u8; 32];
        fs::write(&path, [first, second].concat()).unwrap();
        let file = File::open(path).unwrap();

        assert_eq!(pubkey_at(&file, 1, 2).unwrap(), first);
        assert_eq!(pubkey_at(&file, 2, 2).unwrap(), second);
        assert!(pubkey_at(&file, 0, 2).is_err());
        assert!(pubkey_at(&file, 3, 2).is_err());
    }

    #[test]
    fn public_query_json_uses_signer_user_names() {
        let result = UserProgramQueryResult::from(QueryResult {
            wallet: "signer".into(),
            epoch: 900,
            index_wallet_count: 12,
            index_program_count: 34,
            programs: vec!["program".into()],
        });
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["user"], "signer");
        assert_eq!(value["index_user_count"], 12);
        assert!(value.get("wallet").is_none());
        assert!(value.get("index_wallet_count").is_none());
    }

    #[test]
    fn wallet_decode_requires_exactly_32_bytes() {
        let key = [7u8; 32];
        assert_eq!(
            decode_wallet(&bs58::encode(key).into_string()).unwrap(),
            key
        );
        assert!(decode_wallet("short").is_err());
    }

    #[test]
    fn published_query_rejects_oversized_generation_manifest_before_reading_it() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = query_fixture(directory.path());
        let manifest_path = fixture.archive.join(GENERATION_MANIFEST_FILE);
        File::create(&manifest_path)
            .unwrap()
            .set_len(MAX_GENERATION_MANIFEST_BYTES + 1)
            .unwrap();

        let error = query_index(
            &fixture.index,
            &fixture.archive,
            &bs58::encode(fixture.wallet).into_string(),
            false,
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("above the") && message.contains("byte limit"),
            "{message}"
        );
    }

    #[test]
    fn published_query_rejects_fifo_generation_manifest_without_blocking() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = query_fixture(directory.path());
        let manifest_path = fixture.archive.join(GENERATION_MANIFEST_FILE);
        fs::remove_file(&manifest_path).unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&manifest_path)
                .status()
                .unwrap()
                .success()
        );

        let (sender, receiver) = std::sync::mpsc::channel();
        let index = fixture.index.clone();
        let archive = fixture.archive.clone();
        let wallet = bs58::encode(fixture.wallet).into_string();
        let worker = std::thread::spawn(move || {
            sender
                .send(query_index(&index, &archive, &wallet, false))
                .unwrap();
        });
        let result = match receiver.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(result) => result,
            Err(error) => {
                // If this regresses to a blocking read-only FIFO open, pair it
                // with a nonblocking peer so the worker can unwind cleanly.
                let _peer = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&manifest_path)
                    .unwrap();
                let _ = receiver.recv_timeout(std::time::Duration::from_secs(2));
                worker.join().unwrap();
                panic!("query blocked while opening generation-manifest FIFO: {error}");
            }
        };
        worker.join().unwrap();
        let error = result.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("not a regular file"), "{message}");
    }

    #[test]
    fn relocated_archive_rejects_same_size_altered_registry_with_matching_mphf() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = query_fixture(directory.path());
        let wallet = bs58::encode(fixture.wallet).into_string();
        let expected_program = bs58::encode(fixture.program).into_string();
        assert_eq!(
            query_index(&fixture.index, &fixture.archive, &wallet, false)
                .unwrap()
                .programs,
            vec![expected_program]
        );

        let relocated = directory.path().join("relocated-archive");
        let copied_index = directory.path().join("copied-index");
        copy_fixture_index(&fixture.index, &copied_index);
        fs::create_dir(&relocated).unwrap();
        fs::copy(
            fixture.archive.join(GENERATION_MANIFEST_FILE),
            relocated.join(GENERATION_MANIFEST_FILE),
        )
        .unwrap();

        let mut replacement_wallet = None;
        for marker in 3u8..=u8::MAX {
            let candidate = [marker; 32];
            write_registry_pair(&relocated, &[candidate, fixture.program]);
            if fs::metadata(relocated.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
                .unwrap()
                .len()
                == fixture.registry_index_size
            {
                replacement_wallet = Some(candidate);
                break;
            }
        }
        let replacement_wallet = replacement_wallet.expect("find same-size matching MPHF fixture");
        assert_eq!(
            fs::metadata(relocated.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))
                .unwrap()
                .len(),
            64
        );

        let error = query_index(
            &copied_index,
            &relocated,
            &bs58::encode(replacement_wallet).into_string(),
            false,
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("registry.bin") && message.contains("SHA-256"),
            "{message}"
        );
    }

    #[test]
    fn relocated_archive_rejects_same_size_mphf_mutation() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = query_fixture(directory.path());
        let relocated = directory.path().join("relocated-archive");
        fs::create_dir(&relocated).unwrap();
        for name in [
            GENERATION_MANIFEST_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ] {
            fs::copy(fixture.archive.join(name), relocated.join(name)).unwrap();
        }
        let mphf_path = relocated.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let mut mphf = fs::read(&mphf_path).unwrap();
        let last = mphf.last_mut().unwrap();
        *last ^= 1;
        fs::write(&mphf_path, &mphf).unwrap();
        assert_eq!(mphf.len() as u64, fixture.registry_index_size);

        let error = query_index(
            &fixture.index,
            &relocated,
            &bs58::encode(fixture.wallet).into_string(),
            false,
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("registry.mphf") && message.contains("SHA-256"),
            "{message}"
        );
    }
}
