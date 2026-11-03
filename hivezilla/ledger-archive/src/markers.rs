use anyhow::{Context, Result};
use of_car_reader::blockmarker::{self, Marker};

#[derive(Debug, Default)]
pub(crate) struct RewardNames {
    pub skips: Vec<u64>,
    pub notars: Vec<u64>,
}

/// Skip and notarization reward slots named by the footers of one carrier slot.
pub(crate) fn reward_names(markers: &[Vec<u8>]) -> Result<RewardNames> {
    let mut names = RewardNames::default();
    for marker in markers {
        let parsed = blockmarker::parse(marker).context("block marker does not parse")?;
        let Marker::Footer(footer) = parsed else {
            continue;
        };
        let certificates = blockmarker::decode_footer_certificates(&footer.certificates)
            .context("footer certificates do not parse")?;
        if let Some(skip) = certificates.skip_reward {
            names.skips.push(skip.slot);
        }
        if let Some(notar) = certificates.notar_reward {
            names.notars.push(notar.slot);
        }
    }
    Ok(names)
}

/// Bank hash from the last footer. A block with no footer returns `None`.
pub(crate) fn footer_bank_hash(markers: &[Vec<u8>]) -> Result<Option<[u8; 32]>> {
    let mut bank_hash = None;
    for marker in markers {
        let parsed = blockmarker::parse(marker).context("block marker does not parse")?;
        let Marker::Footer(footer) = parsed else {
            continue;
        };
        blockmarker::decode_footer_certificates(&footer.certificates)
            .context("footer certificates do not parse")?;
        bank_hash = Some(footer.bank_hash);
    }
    Ok(bank_hash)
}
