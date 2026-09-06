use crate::{CompactId, DecodeOutcome, Program, ProgramRole, decode_program};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgramSpec {
    pub program: Program,
    pub address: &'static str,
    pub label: &'static str,
    pub role: ProgramRole,
}

pub const PROGRAM_SPECS: &[ProgramSpec] = &[
    ProgramSpec {
        program: Program::RaydiumClmm,
        address: "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        label: "Raydium CLMM",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::RaydiumCpmm,
        address: "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        label: "Raydium CPMM",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::RaydiumAmmV4,
        address: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        label: "Raydium AMM v4",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OrcaWhirlpool,
        address: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        label: "Orca Whirlpool",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::MeteoraDlmm,
        address: "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        label: "Meteora DLMM",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::MeteoraDammV2,
        address: "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
        label: "Meteora DAMM v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::PancakeSwap,
        address: "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
        label: "PancakeSwap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::JupiterV6,
        address: "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        label: "Jupiter v6",
        role: ProgramRole::Router,
    },
    ProgramSpec {
        program: Program::PumpSwap,
        address: "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        label: "PumpSwap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OkxRouterV2,
        address: "6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma",
        label: "OKX DEX Router v2",
        role: ProgramRole::Router,
    },
    ProgramSpec {
        program: Program::OkxRouterV3,
        address: "proVF4pMXVaYqmy4NjniPh4pqKNfMmsihgd4wdkCX3u",
        label: "OKX DEX Router v3",
        role: ProgramRole::Router,
    },
    ProgramSpec {
        program: Program::RaydiumRoute,
        address: "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS",
        label: "Raydium AMM routing",
        role: ProgramRole::Router,
    },
    ProgramSpec {
        program: Program::LifinityV2,
        address: "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",
        label: "Lifinity v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::RaydiumStable,
        address: "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",
        label: "Raydium Stable Swap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::PumpFun,
        address: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        label: "Pump.fun",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OrcaV2,
        address: "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP",
        label: "Orca v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Legacy9tke,
        address: "9tKE7Mbmj4mxDjWatikzGAtkoWosiiZX9y6J4Hfm2R8H",
        label: "Unidentified legacy swap 9tKE",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Aldrin,
        address: "AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6",
        label: "Aldrin",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::BonkSwap,
        address: "BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p",
        label: "BonkSwap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::CremaClmm,
        address: "CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR",
        label: "Crema CLMM",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::LegacyCtma,
        address: "CTMAxxk34HjKWxQ3QLZK1HpaLXmBveao3ESePXbiyfzh",
        label: "Unidentified legacy swap CTMA",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::AldrinV2,
        address: "CURVGoZn8zycx6FXwwevgBTB2gVvdbGTEpvMJDbgs2t4",
        label: "Aldrin v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::LegacyD3bb,
        address: "D3BBjqUdCYuP18fNvvMbPAZ8DpcRi4io2EsYHQawJDag",
        label: "Unidentified legacy swap D3BB",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OneDex,
        address: "DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm",
        label: "1DEX",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OrcaV1,
        address: "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1",
        label: "Orca v1",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::StepnDex,
        address: "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j",
        label: "STEPN DEX",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::MeteoraPools,
        address: "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
        label: "Meteora pools",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Fluxbeam,
        address: "FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X",
        label: "FluxBeam",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Cropper,
        address: "H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt",
        label: "Cropper",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Invariant,
        address: "HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt",
        label: "Invariant",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::RaydiumLaunchlab,
        address: "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj",
        label: "Raydium LaunchLab",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Phoenix,
        address: "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",
        label: "Phoenix",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Byreal,
        address: "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2",
        label: "Byreal",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Saros,
        address: "SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr",
        label: "Saros",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::StepFinanceSwap,
        address: "SSwpMgqNDsyV7mAgN9ady4bDVu5ySjmmXejXvy2vLt1",
        label: "Step Finance Swap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Saber,
        address: "SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ",
        label: "Saber",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::SolFi,
        address: "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe",
        label: "SolFi",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::ZeroFi,
        address: "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY",
        label: "ZeroFi",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::MeteoraDbc,
        address: "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN",
        label: "Meteora Dynamic Bonding Curve",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::ObricV2,
        address: "obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y",
        label: "Obric v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OpenBookV2,
        address: "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb",
        label: "OpenBook v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::PlasmaGavel,
        address: "srAMMzfVHVAtgSJc8iH6CfKzuWuUTzLHVCE81QU1rgi",
        label: "Plasma (Gavel)",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::StabbleWeighted,
        address: "swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW",
        label: "Stabble weighted swap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::StabbleStable,
        address: "swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ",
        label: "Stabble stable swap",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::RaydiumLegacyV2,
        address: "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",
        label: "Raydium legacy liquidity pool v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::SymmetryV2,
        address: "2KehYt3KsEQR53jYcxjbQp2d2kCp4AkuQW68atufRwSr",
        label: "Symmetry v2",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Legacy2Nz,
        address: "2NZ9rBZtrMdJhwCDYbHjTqAjTQ4bcHxYXFAjsj6NECue",
        label: "Unidentified legacy swap 2NZ9",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::CremaFinance,
        address: "6MLxLqiXaaSUpkgMnWDTuejNZEz3kE7k2woyHGVFw319",
        label: "Crema Finance",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::GooseFxSsl,
        address: "7WduLbRfYhTJktjLw5FDEyrqoEv61aTTCuGAetgLjzN5",
        label: "GooseFX SSL",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::SerumDexV3,
        address: "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
        label: "Serum DEX v3",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::LifinityV1,
        address: "EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S",
        label: "Lifinity v1",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::GooseFxV2,
        address: "GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn",
        label: "GooseFX v2 single-token pools",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::PenguinFinance,
        address: "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP",
        label: "Penguin Finance",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Sencha,
        address: "SCHAtsf8mbjyjiv4LkhLKutTf6JnZAbdJKFkXQNMFHZ",
        label: "Sencha",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Cykura,
        address: "cysPXAjehMpVKUapzbMCCnpFxUFFryEWEaLgnb9NrR8",
        label: "Cykura",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::Dradex,
        address: "dp2waEWSBy5yKmq65ergoU3G6qRLmqa6K7We4rZSKph",
        label: "Dradex",
        role: ProgramRole::Venue,
    },
    ProgramSpec {
        program: Program::OpenBookV1,
        address: "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",
        label: "OpenBook v1",
        role: ProgramRole::Venue,
    },
];

#[derive(Debug, Clone)]
pub struct DispatchTable {
    by_registry_id: Vec<Option<Program>>,
}

impl DispatchTable {
    pub fn from_resolver<F>(registry_len: usize, mut resolve: F) -> Self
    where
        F: FnMut(&str) -> Option<CompactId>,
    {
        let mut table = Self {
            by_registry_id: Vec::new(),
        };
        table.rebind(registry_len, &mut resolve);
        table
    }

    /// Reuses the dense lookup allocation for a new archive registry.
    pub fn rebind<F>(&mut self, registry_len: usize, mut resolve: F)
    where
        F: FnMut(&str) -> Option<CompactId>,
    {
        self.by_registry_id.clear();
        self.by_registry_id.resize(registry_len, None);
        for spec in PROGRAM_SPECS {
            let Some(id) = resolve(spec.address) else {
                continue;
            };
            let Ok(index) = usize::try_from(id) else {
                continue;
            };
            if let Some(entry) = self.by_registry_id.get_mut(index) {
                *entry = Some(spec.program);
            }
        }
    }

    #[inline]
    pub fn program(&self, registry_id: CompactId) -> Option<Program> {
        self.by_registry_id
            .get(usize::try_from(registry_id).ok()?)
            .copied()
            .flatten()
    }

    #[inline]
    pub fn decode(
        &self,
        registry_id: CompactId,
        data: &[u8],
        accounts: &[CompactId],
    ) -> DecodeOutcome {
        let Some(program) = self.program(registry_id) else {
            return DecodeOutcome::UnknownProgram;
        };
        decode_program(program, data, accounts)
    }

    pub fn len(&self) -> usize {
        self.by_registry_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_registry_id.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_is_dense_and_resolves_only_once() {
        let mut calls = 0;
        let table = DispatchTable::from_resolver(8, |address| {
            calls += 1;
            (address == PROGRAM_SPECS[0].address).then_some(3)
        });
        assert_eq!(calls, PROGRAM_SPECS.len());
        assert_eq!(table.program(3), Some(Program::RaydiumClmm));
        assert_eq!(table.program(7), None);
        assert_eq!(table.program(u32::MAX), None);
    }

    #[test]
    fn rebind_reuses_capacity() {
        let mut table = DispatchTable::from_resolver(16, |_| None);
        let pointer = table.by_registry_id.as_ptr();
        let capacity = table.by_registry_id.capacity();
        table.rebind(8, |address| {
            (address == PROGRAM_SPECS[1].address).then_some(5)
        });
        assert_eq!(table.by_registry_id.as_ptr(), pointer);
        assert_eq!(table.by_registry_id.capacity(), capacity);
        assert_eq!(table.program(5), Some(PROGRAM_SPECS[1].program));
    }

    #[test]
    fn program_specs_are_unique_and_roles_match_programs() {
        assert_eq!(core::mem::size_of::<Program>(), 1);
        assert_eq!(core::mem::size_of::<Option<Program>>(), 1);
        for (index, spec) in PROGRAM_SPECS.iter().enumerate() {
            assert_eq!(spec.role, spec.program.role());
            assert!(!spec.address.is_empty());
            assert!(!spec.label.is_empty());
            for other in &PROGRAM_SPECS[index + 1..] {
                assert_ne!(spec.program, other.program);
                assert_ne!(spec.address, other.address);
            }
        }
    }
}
