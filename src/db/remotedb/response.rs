use std::ops::Deref;

use crate::error::KGDataError;

use super::ipcdeser;

#[derive(Debug)]
pub enum Response<'s> {
    Error,
    // contains value, empty if there is no value
    SuccessGet(&'s [u8]),
    // contains values, for each item, it's empty if there is no value
    SuccessBatchGet(Vec<&'s [u8]>),
    // compressed version of batch get
    SuccessCompressedBatchGet((Vec<(usize, usize)>, Vec<u8>)),
    // whether the key exists
    SuccessContains(bool),
    // for testing
    SuccessTest(u32),
}

impl<'s> Response<'s> {
    pub const ERROR: u8 = 0;
    pub const SUCCESS_GET: u8 = 1;
    pub const SUCCESS_BATCH_GET: u8 = 2;
    pub const SUCCESS_COMPRESSED_BATCH_GET: u8 = 3;
    pub const SUCCESS_CONTAINS: u8 = 4;
    pub const SUCCESS_TEST: u8 = 10;

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Response::ERROR => Ok(Self::Error),
            Response::SUCCESS_GET => Ok(Self::SuccessGet(&buf[1..])),
            Response::SUCCESS_BATCH_GET => {
                Ok(Self::SuccessBatchGet(ipcdeser::deserialize_lst(buf)))
            }
            Response::SUCCESS_CONTAINS => Ok(Self::SuccessContains(buf[1] == 1)),
            Response::SUCCESS_TEST => {
                Ok(Self::SuccessTest(u32::from_le_bytes(buf[1..5].try_into()?)))
            }
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline]
    pub fn ser_error(buf: &mut impl ipcdeser::Buffer) -> usize {
        buf.write_byte(Response::ERROR);
        1
    }

    #[inline]
    pub fn ser_success_get<V: Deref<Target = [u8]>>(
        value: V,
        buf: &mut impl ipcdeser::Buffer,
    ) -> usize {
        buf.write_byte(Response::SUCCESS_GET);
        buf.write(&value);
        1 + value.len()
    }

    #[inline]
    pub fn ser_success_contains(value: bool, buf: &mut impl ipcdeser::Buffer) -> usize {
        buf.write(&[Response::SUCCESS_CONTAINS, value as u8]);
        2
    }

    #[inline]
    pub fn ser_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: impl Iterator<Item = V> + ExactSizeIterator,
        buf: &mut impl ipcdeser::Buffer,
    ) -> usize {
        ipcdeser::serialize_iter_to_buffer(Response::SUCCESS_BATCH_GET, values, buf)
    }

    #[inline]
    pub fn ser_compressed_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: impl Iterator<Item = V> + ExactSizeIterator,
        buf: &mut impl ipcdeser::Buffer,
    ) -> usize {
        ipcdeser::serialize_iter_to_buffer(Response::SUCCESS_BATCH_GET, values, buf)
    }

    #[inline]
    pub fn ser_success_test(value: u32, buf: &mut impl ipcdeser::Buffer) -> usize {
        buf.write_byte(Response::SUCCESS_TEST);
        buf.write(&value.to_le_bytes());
        5
    }
}
