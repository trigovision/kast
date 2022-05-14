use serde::{de::DeserializeOwned, Serialize};
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

pub trait Decoder {
    fn decode<T>(&self, data: &T) -> Vec<u8>
    where
        T: Serialize;
}

// impl<F, S> Decoder for F
// where
//     S: Serialize,
//     F: FnOnce(&S) -> Vec<u8> + Copy,
// {
//     fn decode<T>(&self, data: &T) -> Vec<u8>
//     where
//         T: Serialize,
//     {
//         (self)(data)
//     }
// }

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

pub struct JsonDecoder {}
impl JsonDecoder {
    pub fn new() -> Self {
        Self {}
    }
}
impl Decoder for JsonDecoder {
    fn decode<T>(&self, data: &T) -> Vec<u8>
    where
        T: Serialize,
    {
        serde_json::to_vec(&data).unwrap()
    }
}
