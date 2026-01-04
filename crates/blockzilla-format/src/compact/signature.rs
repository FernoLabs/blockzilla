use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, Visitor},
};
use std::fmt;
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
pub struct Signature<'a>(pub &'a [u8; 64]);

impl<'a> Serialize for Signature<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0[..])
    }
}

struct SigVisitor<'a>(std::marker::PhantomData<&'a ()>);

impl<'de, 'a> Visitor<'de> for SigVisitor<'a>
where
    'de: 'a,
{
    type Value = Signature<'a>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("exactly 64 bytes")
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        let arr_de: &'de [u8; 64] = v
            .try_into()
            .map_err(|_| E::invalid_length(v.len(), &"expected 64 bytes"))?;

        // shrink &'de to &'a because 'de: 'a
        let arr_a: &'a [u8; 64] = arr_de;

        Ok(Signature(arr_a))
    }
}

impl<'de, 'a> Deserialize<'de> for Signature<'a>
where
    'de: 'a,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SigVisitor(std::marker::PhantomData))
    }
}
