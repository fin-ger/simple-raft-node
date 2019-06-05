use rmp_serde::{encode, decode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use failure::Fail;

use std::io::Read;

#[derive(Fail, Debug)]
pub enum SerdeError {
    #[fail(display = "Serialization failed")]
    Serialization(encode::Error),
    #[fail(display = "Deserialization failed")]
    Deserialization(decode::Error),
}

pub fn serialize<T: ?Sized + Serialize>(value: &T) -> Result<Vec<u8>, SerdeError> {
    encode::to_vec(value).map_err(|e| SerdeError::Serialization(e))
}

pub fn deserialize<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, SerdeError> {
    decode::from_slice(data).map_err(|e| SerdeError::Deserialization(e))
}

pub fn deserialize_from<R: Read, T: DeserializeOwned>(reader: R) -> Result<T, SerdeError> {
    decode::from_read(reader).map_err(|e| SerdeError::Deserialization(e))
}
