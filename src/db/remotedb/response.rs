use crate::error::KGDataError;

use super::request::{deserialize_bytes, serialize_bytes};

#[derive(Debug)]
pub enum Response<'s> {
    Error,
    SuccessStop,
    // contains value, empty if there is no value
    SuccessGet(&'s [u8]),
    // contains values, for each item, it's empty if there is no value
    SuccessBatchGet(Vec<&'s [u8]>),
    // whether the key exists
    SuccessContains(bool),
}

impl<'s> Response<'s> {
    pub const ERROR: u8 = 0;
    pub const SUCCESS_STOP: u8 = 1;
    pub const SUCCESS_GET: u8 = 2;
    pub const SUCCESS_BATCH_GET: u8 = 3;
    pub const SUCCESS_CONTAINS: u8 = 4;

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Response::ERROR => Ok(Self::Error),
            Response::SUCCESS_STOP => Ok(Self::SuccessStop),
            Response::SUCCESS_GET => Ok(Self::SuccessGet(&buf[1..])),
            Response::SUCCESS_BATCH_GET => Ok(Self::SuccessBatchGet(deserialize_bytes(buf))),
            Response::SUCCESS_CONTAINS => Ok(Self::SuccessContains(buf[1] == 1)),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Error => vec![Response::ERROR],
            Self::SuccessStop => vec![Response::SUCCESS_STOP],
            Self::SuccessGet(value) => {
                let mut buf = Vec::with_capacity(value.len() + 1);
                buf.push(Response::SUCCESS_GET);
                buf.extend_from_slice(value);
                buf
            }
            Self::SuccessBatchGet(values) => serialize_bytes(Response::SUCCESS_BATCH_GET, values),
            Self::SuccessContains(value) => vec![Response::SUCCESS_CONTAINS, *value as u8],
        }
    }
}
