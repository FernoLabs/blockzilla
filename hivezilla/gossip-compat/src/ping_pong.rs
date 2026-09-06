use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use solana_address::Address as Pubkey;
use solana_hash::Hash;
use solana_keypair::{Keypair, Signer, signable::Signable};
use solana_sha256_hasher::hashv;
use solana_signature::Signature;

#[derive(Debug, Deserialize, Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
pub struct Ping<const N: usize> {
    from: Pubkey,
    #[serde(with = "BigArray")]
    token: [u8; N],
    signature: Signature,
}

#[derive(Debug, Deserialize, Serialize, wincode::SchemaRead, wincode::SchemaWrite)]
pub struct Pong {
    from: Pubkey,
    hash: Hash,
    signature: Signature,
}

impl<const N: usize> Ping<N> {
    pub fn new(token: [u8; N], keypair: &Keypair) -> Self {
        let signature = keypair.sign_message(&token);
        Self {
            from: keypair.pubkey(),
            token,
            signature,
        }
    }

    pub fn from(&self) -> &Pubkey {
        &self.from
    }
}

impl<const N: usize> Signable for Ping<N> {
    fn pubkey(&self) -> Pubkey {
        self.from
    }

    fn signable_data(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.token)
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature;
    }
}

impl Pong {
    pub fn new<const N: usize>(ping: &Ping<N>, keypair: &Keypair) -> Self {
        let hash = hash_ping_token(&ping.token);
        Self {
            from: keypair.pubkey(),
            hash: hash.clone(),
            signature: keypair.sign_message(hash.as_ref()),
        }
    }

    pub fn from(&self) -> &Pubkey {
        &self.from
    }

    pub(crate) fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl Signable for Pong {
    fn pubkey(&self) -> Pubkey {
        self.from
    }

    fn signable_data(&self) -> Cow<'static, [u8]> {
        Cow::Owned(self.hash.as_ref().into())
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature;
    }
}

fn hash_ping_token<const N: usize>(token: &[u8; N]) -> Hash {
    const PING_PONG_HASH_PREFIX: &[u8] = b"SOLANA_PING_PONG";
    hashv(&[PING_PONG_HASH_PREFIX, token])
}
