use serde::de::DeserializeOwned;
use std::marker::PhantomData;

pub trait Encoder {
    type In;

    fn encode(&self, data: Option<&[u8]>) -> Self::In;
}

impl<T, F> Encoder for F
where
    F: FnOnce(Option<&[u8]>) -> T + Copy,
{
    type In = T;

    fn encode(&self, data: Option<&[u8]>) -> Self::In {
        (self)(data)
    }
}

#[derive(Clone)]
pub struct JsonEncoder<T> {
    _marker: PhantomData<T>,
}

impl<T> JsonEncoder<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Encoder for JsonEncoder<T>
where
    T: DeserializeOwned,
{
    type In = T;

    fn encode(&self, data: Option<&[u8]>) -> Self::In {
        serde_json::from_slice(data.expect("empty message")).unwrap()
    }
}
