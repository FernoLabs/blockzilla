//! Identical application output for the independent CAR and Jetstreamer probes.
//! See README.md for the binary layout and limits of the comparison.
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

pub const SCHEMA: &str = "car-common-export-v1";
const MAGIC: &[u8; 16] = b"CAR-TX-EXPORT-01";
const MAX_BLOCK: usize = 64 << 20;

pub struct Transaction<'a> {
    pub slot: u64,
    pub index: u64,
    pub signature: &'a [u8],
    pub message_hash: &'a [u8],
    pub vote: bool,
    pub failed: bool,
    pub fee: u64,
    pub pre_balances: &'a [u64],
    pub post_balances: &'a [u64],
}

pub fn encode(out: &mut Vec<u8>, tx: Transaction<'_>) -> io::Result<()> {
    if tx.signature.len() != 64 || tx.message_hash.len() != 32 {
        return Err(io::Error::other("invalid signature or message hash length"));
    }
    let extra = tx
        .pre_balances
        .len()
        .checked_add(tx.post_balances.len())
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(138))
        .ok_or_else(|| io::Error::other("export length overflow"))?;
    if out.len().checked_add(extra).is_none_or(|n| n > MAX_BLOCK) {
        return Err(io::Error::other("export block exceeds 64 MiB"));
    }
    out.extend_from_slice(&tx.slot.to_le_bytes());
    out.extend_from_slice(&tx.index.to_le_bytes());
    out.extend_from_slice(tx.signature);
    out.extend_from_slice(tx.message_hash);
    out.extend_from_slice(&[u8::from(tx.vote), u8::from(tx.failed)]);
    out.extend_from_slice(&tx.fee.to_le_bytes());
    for balances in [tx.pre_balances, tx.post_balances] {
        out.extend_from_slice(&(balances.len() as u64).to_le_bytes());
        for balance in balances {
            out.extend_from_slice(&balance.to_le_bytes());
        }
    }
    Ok(())
}

struct Spool {
    file: BufWriter<File>,
    offset: u64,
    // slot -> (transaction count, spool offset, byte length)
    blocks: BTreeMap<u64, (u64, u64, u64)>,
}

pub struct Export {
    spool: Mutex<Spool>,
    scratch: PathBuf,
    output: Mutex<File>,
}

impl Export {
    pub fn create(path: &Path) -> io::Result<Self> {
        let output = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut scratch = path.as_os_str().to_owned();
        scratch.push(".spool");
        let scratch = PathBuf::from(scratch);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&scratch)?;
        Ok(Self {
            spool: Mutex::new(Spool {
                file: BufWriter::with_capacity(1 << 20, file),
                offset: 0,
                blocks: BTreeMap::new(),
            }),
            scratch,
            output: Mutex::new(output),
        })
    }

    pub fn block(&self, slot: u64, count: u64, bytes: &[u8]) -> io::Result<()> {
        let mut spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        if spool.blocks.contains_key(&slot)
            || bytes.len() > MAX_BLOCK
            || (count == 0) != bytes.is_empty()
        {
            return Err(io::Error::other("invalid or duplicate export block"));
        }
        let offset = spool.offset;
        spool.file.write_all(bytes)?;
        spool
            .blocks
            .insert(slot, (count, offset, bytes.len() as u64));
        spool.offset += bytes.len() as u64;
        Ok(())
    }

    /// Validate coverage, write canonical slot order, and sync the final file.
    /// Both probes include this work in end-to-end elapsed time.
    pub fn finish(&self, expected: &[[u64; 2]]) -> io::Result<u64> {
        let mut spool = self
            .spool
            .lock()
            .map_err(|_| io::Error::other("export lock poisoned"))?;
        if expected.is_empty() || !expected.windows(2).all(|r| r[0][0] < r[1][0]) {
            return Err(io::Error::other("invalid export plan"));
        }
        for (&slot, &(count, _, _)) in &spool.blocks {
            if expected
                .binary_search_by_key(&slot, |r| r[0])
                .ok()
                .is_none_or(|i| expected[i][1] != count)
            {
                return Err(io::Error::other(
                    "export contains unexpected block or count",
                ));
            }
        }
        for &[slot, count] in expected {
            if count > 0 && !spool.blocks.contains_key(&slot) {
                return Err(io::Error::other("export is missing a nonempty block"));
            }
        }
        spool.file.flush()?;
        let mut output = self
            .output
            .lock()
            .map_err(|_| io::Error::other("output lock poisoned"))?;
        let mut writer = BufWriter::with_capacity(1 << 20, &mut *output);
        writer.write_all(MAGIC)?;
        for &[slot, count] in expected {
            let (_, offset, len) = spool.blocks.get(&slot).copied().unwrap_or((0, 0, 0));
            for n in [slot, count, len] {
                writer.write_all(&n.to_le_bytes())?;
            }
            spool.file.get_mut().seek(SeekFrom::Start(offset))?;
            if io::copy(&mut spool.file.get_mut().take(len), &mut writer)? != len {
                return Err(io::Error::other("truncated export spool"));
            }
        }
        writer.flush()?;
        drop(writer);
        output.sync_all()?;
        let bytes = output.metadata()?.len();
        std::fs::remove_file(&self.scratch)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_order_and_missing_block_validation() {
        let path = std::env::temp_dir().join(format!("car-export-test-{}", std::process::id()));
        let export = Export::create(&path).unwrap();
        let mut bytes = Vec::new();
        encode(
            &mut bytes,
            Transaction {
                slot: 3,
                index: 0,
                signature: &[1; 64],
                message_hash: &[2; 32],
                vote: true,
                failed: false,
                fee: 5000,
                pre_balances: &[10, 20],
                post_balances: &[5, 25],
            },
        )
        .unwrap();
        assert_eq!(bytes.len(), 170);
        export.block(3, 1, &bytes).unwrap();
        assert!(export.block(3, 1, &bytes).is_err());
        assert!(export.finish(&[[2, 1], [3, 1]]).is_err());
        assert_eq!(export.finish(&[[2, 0], [3, 1]]).unwrap(), 234);
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[..16], MAGIC);
        assert_eq!(u64::from_le_bytes(data[16..24].try_into().unwrap()), 2);
        assert_eq!(&data[64..], &bytes);
        std::fs::remove_file(path).unwrap();
    }
}
