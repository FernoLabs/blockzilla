use core::marker::PhantomData;
use minicbor::decode::Error as CborError;
use minicbor::{Decode, Decoder};

#[derive(Debug, Clone)]
pub struct CborArrayView<'b, T> {
    pub(crate) slice: &'b [u8],
    pub(crate) _t: PhantomData<T>,
}

impl<'b, C, T> Decode<'b, C> for CborArrayView<'b, T> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, CborError> {
        let start = d.position();
        d.skip()?;
        let end = d.position();
        let input = d.input();

        Ok(Self {
            slice: &input[start..end],
            _t: PhantomData,
        })
    }
}

impl<'b, T> CborArrayView<'b, T>
where
    T: Decode<'b, ()>,
{
    pub fn len(&self) -> usize {
        let mut d = Decoder::new(self.slice);
        d.array().unwrap_or(Some(0)).unwrap_or(0) as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<T, CborError>> + 'b {
        let mut d = Decoder::new(self.slice);
        let n = d.array().unwrap_or(Some(0)).unwrap_or(0);
        (0..n).map(move |_| d.decode_with(&mut ()))
    }
}
