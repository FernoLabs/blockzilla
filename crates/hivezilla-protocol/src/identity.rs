use std::fmt;

use crate::{ProtocolError, Result};

macro_rules! fixed_bytes_type {
    ($name:ident, $length:expr, $field:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name([u8; $length]);

        impl $name {
            pub const LENGTH: usize = $length;

            #[must_use]
            pub const fn new(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }

            #[must_use]
            pub const fn into_bytes(self) -> [u8; $length] {
                self.0
            }
        }

        impl From<[u8; $length]> for $name {
            fn from(value: [u8; $length]) -> Self {
                Self::new(value)
            }
        }

        impl TryFrom<&[u8]> for $name {
            type Error = ProtocolError;

            fn try_from(value: &[u8]) -> Result<Self> {
                let bytes = value.try_into().map_err(|_| ProtocolError::InvalidLength {
                    field: $field,
                    expected: $length,
                    actual: value.len(),
                })?;
                Ok(Self::new(bytes))
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                write_hex(formatter, &self.0)?;
                formatter.write_str(")")
            }
        }
    };
}

fn write_hex(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

fixed_bytes_type!(StreamId, 16, "stream_id");
fixed_bytes_type!(ClusterGenesisHash, 32, "cluster_genesis_hash");
fixed_bytes_type!(ProducerConfigSha256, 32, "producer_config_sha256");
fixed_bytes_type!(StreamManifestSha256, 32, "stream_manifest_sha256");
fixed_bytes_type!(PrefixHash, 32, "prefix_hash");
fixed_bytes_type!(DurabilityTargetId, 16, "durability_target_id");
fixed_bytes_type!(FailureDomainId, 16, "failure_domain_id");
fixed_bytes_type!(
    DurabilityTargetDescriptorSha256,
    32,
    "target_descriptor_sha256"
);
fixed_bytes_type!(DurabilityPolicyId, 16, "durability_policy_id");
fixed_bytes_type!(
    TerminalCatalogDescriptorSha256,
    32,
    "catalog_descriptor_sha256"
);
fixed_bytes_type!(OverflowNamespaceSha256, 32, "overflow_namespace_sha256");
fixed_bytes_type!(
    DeletionAuthorizingStoreId,
    16,
    "deletion_authorizing_store_id"
);
fixed_bytes_type!(BlockzillaAuthorityId, 16, "blockzilla_authority_id");
fixed_bytes_type!(
    StreamRegistrySnapshotSha256,
    32,
    "stream_registry_snapshot_sha256"
);
fixed_bytes_type!(SessionId, 16, "session_id");
fixed_bytes_type!(AcceptedAckReceiptSha256, 32, "accepted_ack_receipt_sha256");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_identity_rejects_the_wrong_length() {
        assert_eq!(
            StreamId::try_from(&[0_u8; 15][..]),
            Err(ProtocolError::InvalidLength {
                field: "stream_id",
                expected: 16,
                actual: 15,
            })
        );
    }

    #[test]
    fn debug_is_unambiguous_lowercase_hex() {
        assert_eq!(
            format!("{:?}", StreamId::new([0xab; 16])),
            "StreamId(abababababababababababababababab)"
        );
    }
}
