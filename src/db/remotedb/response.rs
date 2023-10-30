use crate::error::KGDataError;

use super::ipcdeser;

#[derive(Debug)]
pub enum Response<'s> {
    Error,
    // contains value, empty if there is no value
    SuccessGet(&'s [u8]),
    // contains values, for each item, it's empty if there is no value
    SuccessBatchGet(Vec<&'s [u8]>),
    // whether the key exists
    SuccessContains(bool),
}

impl<'s> Response<'s> {
    pub const ERROR: u8 = 0;
    pub const SUCCESS_GET: u8 = 1;
    pub const SUCCESS_BATCH_GET: u8 = 2;
    pub const SUCCESS_CONTAINS: u8 = 3;

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Response::ERROR => Ok(Self::Error),
            Response::SUCCESS_GET => Ok(Self::SuccessGet(&buf[1..])),
            Response::SUCCESS_BATCH_GET => {
                Ok(Self::SuccessBatchGet(ipcdeser::deserialize_lst(buf)))
            }
            Response::SUCCESS_CONTAINS => Ok(Self::SuccessContains(buf[1] == 1)),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[allow(dead_code)]
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Error => vec![Response::ERROR],
            Self::SuccessGet(value) => {
                let mut buf = Vec::with_capacity(value.len() + 1);
                buf.push(Response::SUCCESS_GET);
                buf.extend_from_slice(value);
                buf
            }
            Self::SuccessBatchGet(values) => {
                ipcdeser::serialize_lst(Response::SUCCESS_BATCH_GET, values)
            }
            Self::SuccessContains(value) => vec![Response::SUCCESS_CONTAINS, *value as u8],
        }
    }

    pub fn serialize_to_buf(&self, buf: &mut impl ipcdeser::Buffer) -> usize {
        match self {
            Self::Error => {
                buf.write_byte(Response::ERROR);
                1
            }
            Self::SuccessGet(value) => {
                buf.write_byte(Response::SUCCESS_GET);
                buf.write(value);
                1 + value.len()
            }
            Self::SuccessBatchGet(values) => {
                ipcdeser::serialize_lst_to_buffer(Response::SUCCESS_BATCH_GET, values, buf)
            }
            Self::SuccessContains(value) => {
                buf.write_byte(Response::SUCCESS_CONTAINS);
                buf.write_byte(*value as u8);
                2
            }
        }
    }
}
