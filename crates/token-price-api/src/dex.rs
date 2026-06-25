use crate::format::{
    NO_ID, SWAP_FLAG_KNOWN_DEX, SWAP_FLAG_QUOTE_IN, SWAP_FLAG_QUOTE_OUT, SWAP_FLAG_TX_ERROR,
    SwapRecord, TokenBalanceChangeRecord, price_micros,
};
use blockzilla_format::{ArchiveV2HotInstruction, ArchiveV2HotMessagePayload};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexSource {
    Carbon,
    Vixen,
    Birdeye,
    Manual,
}

#[derive(Debug, Clone, Copy)]
pub struct DexProgramSpec {
    pub slug: &'static str,
    pub label: &'static str,
    pub program_id: &'static str,
    pub sources: &'static [DexSource],
}

#[derive(Debug, Clone)]
pub struct ResolvedDexProgram {
    pub slug: String,
    pub label: String,
    pub program_id: u32,
    pub address: String,
    pub sources: Vec<DexSource>,
}

impl ResolvedDexProgram {
    pub fn custom(program_id: u32, address: String) -> Self {
        Self {
            slug: format!("custom:{address}"),
            label: "Custom DEX".to_string(),
            program_id,
            address,
            sources: vec![DexSource::Manual],
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DexTxContext<'a> {
    pub slot: u64,
    pub block_time: i64,
    pub tx_index: u32,
    pub block_id: u32,
    pub signature_id: u32,
    pub tx_error: bool,
    pub message: &'a ArchiveV2HotMessagePayload,
    pub account_key_ids: &'a [Option<u32>],
    pub touched_program_ids: &'a BTreeSet<u32>,
    pub quote_mint_ids: &'a BTreeSet<u32>,
    pub balance_changes: &'a [TokenBalanceChangeRecord],
}

impl<'a> DexTxContext<'a> {
    pub fn instructions(&self) -> &'a [ArchiveV2HotInstruction] {
        match self.message {
            ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
            ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
        }
    }

    pub fn instruction_program_id(&self, instruction: &ArchiveV2HotInstruction) -> Option<u32> {
        self.account_key_ids
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
    }
}

pub trait DexDecoder: Send + Sync {
    fn name(&self) -> &'static str;
    fn decode(&self, registry: &DexRegistry, ctx: DexTxContext<'_>, out: &mut Vec<SwapRecord>);
}

pub struct DexRegistry {
    programs_by_id: BTreeMap<u32, ResolvedDexProgram>,
    decoders: Vec<Box<dyn DexDecoder>>,
}

impl DexRegistry {
    pub fn new(programs: Vec<ResolvedDexProgram>) -> Self {
        let programs_by_id = programs
            .into_iter()
            .map(|program| (program.program_id, program))
            .collect();
        let mut registry = Self {
            programs_by_id,
            decoders: Vec::new(),
        };
        registry.register_decoder(BalanceDeltaDexDecoder::default());
        registry
    }

    pub fn register_program(&mut self, program: ResolvedDexProgram) {
        self.programs_by_id.insert(program.program_id, program);
    }

    pub fn extend_programs<I>(&mut self, programs: I)
    where
        I: IntoIterator<Item = ResolvedDexProgram>,
    {
        for program in programs {
            self.register_program(program);
        }
    }

    pub fn register_decoder<D>(&mut self, decoder: D)
    where
        D: DexDecoder + 'static,
    {
        self.decoders.push(Box::new(decoder));
    }

    pub fn programs(&self) -> impl Iterator<Item = &ResolvedDexProgram> {
        self.programs_by_id.values()
    }

    pub fn is_known_program(&self, program_id: u32) -> bool {
        self.programs_by_id.contains_key(&program_id)
    }

    pub fn program(&self, program_id: u32) -> Option<&ResolvedDexProgram> {
        self.programs_by_id.get(&program_id)
    }

    pub fn best_touched_program(&self, touched_program_ids: &BTreeSet<u32>) -> u32 {
        touched_program_ids
            .iter()
            .copied()
            .find(|program_id| self.is_known_program(*program_id))
            .unwrap_or(NO_ID)
    }

    pub fn decode(&self, ctx: DexTxContext<'_>, out: &mut Vec<SwapRecord>) {
        for decoder in &self.decoders {
            decoder.decode(self, ctx, out);
        }
    }
}

impl std::fmt::Debug for DexRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DexRegistry")
            .field("programs", &self.programs_by_id.len())
            .field("decoders", &self.decoders.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BalanceDeltaDexDecoder {
    /// When false, the decoder still emits swaps for clean two-token owner deltas
    /// even if no known DEX program id was detected. This catches fresh venues
    /// before the catalog is updated.
    pub require_known_program: bool,
}

impl Default for BalanceDeltaDexDecoder {
    fn default() -> Self {
        Self {
            require_known_program: false,
        }
    }
}

impl DexDecoder for BalanceDeltaDexDecoder {
    fn name(&self) -> &'static str {
        "balance-delta"
    }

    fn decode(&self, registry: &DexRegistry, ctx: DexTxContext<'_>, out: &mut Vec<SwapRecord>) {
        let dex_program_id = registry.best_touched_program(ctx.touched_program_ids);
        if self.require_known_program && dex_program_id == NO_ID {
            return;
        }

        let mut by_owner: BTreeMap<u32, BTreeMap<u32, NetDelta>> = BTreeMap::new();
        for record in ctx.balance_changes {
            if record.owner_id == NO_ID || record.pre_amount == record.post_amount {
                continue;
            }
            let delta = record.post_amount as i128 - record.pre_amount as i128;
            let entry = by_owner
                .entry(record.owner_id)
                .or_default()
                .entry(record.mint_id)
                .or_insert(NetDelta {
                    delta: 0,
                    decimals: record.decimals,
                });
            entry.delta += delta;
            entry.decimals = record.decimals;
        }

        for (owner_id, deltas) in by_owner {
            let nonzero = deltas
                .into_iter()
                .filter(|(_, delta)| delta.delta != 0)
                .collect::<Vec<_>>();
            if nonzero.len() != 2 {
                continue;
            }
            let (left_mint, left) = nonzero[0];
            let (right_mint, right) = nonzero[1];
            let Some((in_mint_id, input, out_mint_id, output)) =
                swap_sides(left_mint, left, right_mint, right)
            else {
                continue;
            };
            let Ok(amount_in) = u64::try_from(-input.delta) else {
                continue;
            };
            let Ok(amount_out) = u64::try_from(output.delta) else {
                continue;
            };
            if amount_in == 0 || amount_out == 0 {
                continue;
            }

            let quote_in = ctx.quote_mint_ids.contains(&in_mint_id);
            let quote_out = ctx.quote_mint_ids.contains(&out_mint_id);
            let mut flags = 0u16;
            if ctx.tx_error {
                flags |= SWAP_FLAG_TX_ERROR;
            }
            if dex_program_id != NO_ID {
                flags |= SWAP_FLAG_KNOWN_DEX;
            }
            if quote_in {
                flags |= SWAP_FLAG_QUOTE_IN;
            }
            if quote_out {
                flags |= SWAP_FLAG_QUOTE_OUT;
            }
            let price = if quote_out {
                price_micros(amount_in, input.decimals, amount_out, output.decimals)
            } else if quote_in {
                price_micros(amount_out, output.decimals, amount_in, input.decimals)
            } else {
                0
            };
            out.push(SwapRecord {
                slot: ctx.slot,
                block_time: ctx.block_time,
                tx_index: ctx.tx_index,
                block_id: ctx.block_id,
                signature_id: ctx.signature_id,
                owner_id,
                dex_program_id,
                in_mint_id,
                out_mint_id,
                in_decimals: input.decimals,
                out_decimals: output.decimals,
                flags,
                amount_in,
                amount_out,
                price_micros: price,
            });
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NetDelta {
    delta: i128,
    decimals: u8,
}

fn swap_sides(
    left_mint: u32,
    left: NetDelta,
    right_mint: u32,
    right: NetDelta,
) -> Option<(u32, NetDelta, u32, NetDelta)> {
    match (left.delta < 0, right.delta > 0) {
        (true, true) => Some((left_mint, left, right_mint, right)),
        (false, false) if right.delta < 0 && left.delta > 0 => {
            Some((right_mint, right, left_mint, left))
        }
        _ => None,
    }
}

pub const CARBON_SOURCE: &[DexSource] = &[DexSource::Carbon];
pub const CARBON_VIXEN_SOURCE: &[DexSource] = &[DexSource::Carbon, DexSource::Vixen];
pub const CARBON_BIRDEYE_SOURCE: &[DexSource] = &[DexSource::Carbon, DexSource::Birdeye];
pub const CARBON_VIXEN_BIRDEYE_SOURCE: &[DexSource] =
    &[DexSource::Carbon, DexSource::Vixen, DexSource::Birdeye];
pub const MANUAL_SOURCE: &[DexSource] = &[DexSource::Manual];

pub const DEFAULT_DEX_PROGRAMS: &[DexProgramSpec] = &[
    DexProgramSpec {
        slug: "bonkswap",
        label: "Bonkswap",
        program_id: "BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "boop",
        label: "Boop.fun",
        program_id: "boop8hVGQGqehUK2iVEMEnMrL5RbjywRzHKBmBE7ry4",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "dflow-aggregator-v4",
        label: "DFlow Aggregator V4",
        program_id: "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "drift-v2",
        label: "Drift V2",
        program_id: "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "fluxbeam",
        label: "Fluxbeam",
        program_id: "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "gavel",
        label: "Gavel Pool",
        program_id: "srAMMzfVHVAtgSJc8iH6CfKzuWuUTzLHVCE81QU1rgi",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "heaven",
        label: "Heaven",
        program_id: "HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "jupiter-dca",
        label: "Jupiter DCA",
        program_id: "DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "jupiter-limit-order",
        label: "Jupiter Limit Order",
        program_id: "jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "jupiter-limit-order-2",
        label: "Jupiter Limit Order 2",
        program_id: "j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "jupiter-perpetuals",
        label: "Jupiter Perpetuals",
        program_id: "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "jupiter-swap-v6",
        label: "Jupiter Swap v6",
        program_id: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "kamino-limit-order",
        label: "Kamino Limit Order",
        program_id: "LiMoM9rMhrdYrfzUCxQppvxCSG1FcrUK9G8uLq4A1GF",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "lifinity-amm-v2",
        label: "Lifinity AMM V2",
        program_id: "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "marinade-finance",
        label: "Marinade Finance",
        program_id: "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "meteora-damm-v2",
        label: "Meteora DAMM V2",
        program_id: "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "meteora-dbc",
        label: "Meteora DBC",
        program_id: "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "meteora-dlmm",
        label: "Meteora DLMM",
        program_id: "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "meteora-pools",
        label: "Meteora Pools",
        program_id: "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "meteora-vault",
        label: "Meteora Vault",
        program_id: "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "moonshot",
        label: "Moonshot",
        program_id: "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "okx-dex-v2",
        label: "OKX DEX V2",
        program_id: "6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "okx-dex-v3",
        label: "OKX DEX V3",
        program_id: "proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "okx-route",
        label: "OKX Route",
        program_id: "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS",
        sources: MANUAL_SOURCE,
    },
    DexProgramSpec {
        slug: "openbook-v2",
        label: "OpenBook V2",
        program_id: "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "orca-whirlpool",
        label: "Orca Whirlpool",
        program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "pancake-swap",
        label: "Pancake Swap",
        program_id: "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "phoenix-v1",
        label: "Phoenix V1",
        program_id: "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        sources: CARBON_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "pumpfun",
        label: "Pump.fun",
        program_id: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "pump-swap",
        label: "PumpSwap",
        program_id: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "raydium-amm-v4",
        label: "Raydium AMM V4",
        program_id: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "raydium-clmm",
        label: "Raydium CLMM",
        program_id: "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        sources: CARBON_VIXEN_BIRDEYE_SOURCE,
    },
    DexProgramSpec {
        slug: "raydium-cpmm",
        label: "Raydium CPMM",
        program_id: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "raydium-launchpad",
        label: "Raydium Launchpad",
        program_id: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "raydium-stable-swap",
        label: "Raydium Stable Swap",
        program_id: "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "stabble-stable-swap",
        label: "Stabble Stable Swap",
        program_id: "swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "stabble-weighted-swap",
        label: "Stabble Weighted Swap",
        program_id: "swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "vertigo",
        label: "Vertigo",
        program_id: "vrTGoBuy5rYSxAfV3jaRJWHH6nN9WK4NRExGxsk1bCJ",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "virtuals",
        label: "Virtuals",
        program_id: "5U3EU2ubXtK84QcRjWVmYt9RaDyA8gKxdUrPFXmZyaki",
        sources: CARBON_VIXEN_SOURCE,
    },
    DexProgramSpec {
        slug: "wavebreak",
        label: "Wavebreak",
        program_id: "waveQX2yP3H1pVU8djGvEHmYg8uamQ84AuyGtpsrXTF",
        sources: CARBON_SOURCE,
    },
    DexProgramSpec {
        slug: "zeta",
        label: "Zeta",
        program_id: "ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD",
        sources: CARBON_BIRDEYE_SOURCE,
    },
];

pub const BIRDEYE_LEGACY_DEX_NAMES: &[&str] = &[
    "Serum",
    "Aldrin",
    "Step",
    "Saros",
    "Cropper",
    "Crema",
    "Saber",
    "Sencha",
    "Mercurial",
    "Penguin",
    "Solana Swap Program",
    "Onemoon",
    "Cykura",
    "Swim",
    "Dooar",
    "Mango",
    "Symmetry",
    "Invariant",
    "GooseFX",
    "Solanax",
    "DeltaFi",
    "Dradex",
    "Balansol",
    "Marcopolo",
    "Oasis",
    "1sol",
    "Francium",
    "Apricot",
    "Tulip",
    "Prism",
    "OpenOcean",
    "KyberSwap",
    "Elixir",
    "Slow Protocol",
    "LP Finance",
    "Snowflake",
];

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        ArchiveV2HotLegacyMessage, CompactMessageHeader, OwnedCompactRecentBlockhash,
    };

    #[test]
    fn balance_delta_decoder_prices_quote_side_swaps() {
        let registry = DexRegistry::new(vec![ResolvedDexProgram {
            slug: "test-dex".to_string(),
            label: "Test DEX".to_string(),
            program_id: 77,
            address: "test".to_string(),
            sources: vec![DexSource::Manual],
        }]);
        let quote_mints = BTreeSet::from([2u32]);
        let touched = BTreeSet::from([77u32]);
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: Vec::new(),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        let balances = vec![
            TokenBalanceChangeRecord {
                slot: 1,
                block_time: 2,
                tx_index: 3,
                block_id: 4,
                account_id: 10,
                mint_id: 1,
                owner_id: 9,
                program_id: 0,
                pre_amount: 1_000_000,
                post_amount: 0,
                decimals: 6,
                flags: 0,
            },
            TokenBalanceChangeRecord {
                slot: 1,
                block_time: 2,
                tx_index: 3,
                block_id: 4,
                account_id: 11,
                mint_id: 2,
                owner_id: 9,
                program_id: 0,
                pre_amount: 0,
                post_amount: 2_000_000,
                decimals: 6,
                flags: 0,
            },
        ];
        let mut out = Vec::new();
        registry.decode(
            DexTxContext {
                slot: 1,
                block_time: 2,
                tx_index: 3,
                block_id: 4,
                signature_id: 5,
                tx_error: false,
                message: &message,
                account_key_ids: &[],
                touched_program_ids: &touched,
                quote_mint_ids: &quote_mints,
                balance_changes: &balances,
            },
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].dex_program_id, 77);
        assert_eq!(out[0].in_mint_id, 1);
        assert_eq!(out[0].out_mint_id, 2);
        assert_eq!(out[0].price_micros, 2_000_000);
        assert_ne!(out[0].flags & SWAP_FLAG_KNOWN_DEX, 0);
    }
}
