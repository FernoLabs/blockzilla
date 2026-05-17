use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{StrId, StringTable};

pub const STR_ID: &str = "ZkE1Gama1Proof11111111111111111111111111111";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ZkElgamalProofLog {
    Static(ZkElgamalProofStaticLog),
    ProofVerificationFailed { err: StrId },
}

macro_rules! zk_static_logs {
    ($( $variant:ident => $text:literal, )+ ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
        pub enum ZkElgamalProofStaticLog {
            $( $variant, )+
        }

        impl ZkElgamalProofStaticLog {
            #[inline]
            pub fn parse(text: &str) -> Option<Self> {
                match text {
                    $( $text => Some(Self::$variant), )+
                    _ => None,
                }
            }

            #[inline]
            pub fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $text, )+
                }
            }
        }
    };
}

zk_static_logs! {
    InvalidProofData => "invalid proof data",
    ProgramTemporarilyDisabled => "zk-elgamal-proof program is temporarily disabled",
    CloseContextState => "CloseContextState",
    VerifyZeroCiphertext => "VerifyZeroCiphertext",
    VerifyCiphertextCiphertextEquality => "VerifyCiphertextCiphertextEquality",
    VerifyCiphertextCommitmentEquality => "VerifyCiphertextCommitmentEquality",
    VerifyPubkeyValidity => "VerifyPubkeyValidity",
    VerifyPercentageWithCap => "VerifyPercentageWithCap",
    VerifyBatchedRangeProofU64 => "VerifyBatchedRangeProofU64",
    VerifyBatchedRangeProofU128 => "VerifyBatchedRangeProofU128",
    VerifyBatchedRangeProofU256 => "VerifyBatchedRangeProofU256",
    VerifyGroupedCiphertext2HandlesValidity => "VerifyGroupedCiphertext2HandlesValidity",
    VerifyBatchedGroupedCiphertext2HandlesValidity => "VerifyBatchedGroupedCiphertext2HandlesValidity",
    VerifyGroupedCiphertext3HandlesValidity => "VerifyGroupedCiphertext3HandlesValidity",
    VerifyBatchedGroupedCiphertext3HandlesValidity => "VerifyBatchedGroupedCiphertext3HandlesValidity",
}

impl ZkElgamalProofLog {
    #[inline]
    pub fn parse(payload: &str, st: &mut StringTable) -> Option<Self> {
        if let Some(log) = ZkElgamalProofStaticLog::parse(payload) {
            return Some(Self::Static(log));
        }

        if let Some(err) = payload.strip_prefix("proof verification failed: ") {
            return Some(Self::ProofVerificationFailed { err: st.push(err) });
        }

        None
    }

    #[inline]
    pub fn as_str(&self, st: &StringTable) -> String {
        match self {
            Self::Static(log) => log.as_str().to_string(),
            Self::ProofVerificationFailed { err } => {
                format!("proof verification failed: {}", st.resolve(*err))
            }
        }
    }
}
