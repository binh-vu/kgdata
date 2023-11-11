use std::ops::Deref;

use crate::error::KGDataError;

use super::ipcserde;

#[derive(Debug)]
pub enum Request<'s> {
    // Get a key from the database
    Get(&'s [u8]),

    // Get multiple keys from the database
    BatchGet(Vec<&'s [u8]>),

    // Check if a key exists in the database
    Contains(&'s [u8]),

    // For testing
    Test(&'s str),
}

impl<'s> Request<'s> {
    pub const GET: u8 = 0;
    pub const BATCH_GET: u8 = 1;
    pub const CONTAINS: u8 = 2;
    pub const FINISH: u8 = 3;
    pub const TEST: u8 = 10;

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Request::GET => Ok(Self::Get(&buf[1..])),
            Request::BATCH_GET => Ok(Self::BatchGet(ipcserde::deserialize_lst(&buf[1..]))),
            Request::CONTAINS => Ok(Self::Contains(&buf[1..])),
            Request::TEST => Ok(Self::Test(std::str::from_utf8(&buf[1..])?)),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline]
    pub fn ser_get<V: Deref<Target = [u8]>>(key: V) -> Vec<u8> {
        let serkey = &key;
        let mut buf = Vec::with_capacity(key.len() + 1);
        buf.push(Request::GET);
        buf.extend_from_slice(serkey);
        buf
    }

    #[inline]
    pub fn ser_contains<V: Deref<Target = [u8]>>(key: V) -> Vec<u8> {
        let serkey = &key;
        let mut buf = Vec::with_capacity(key.len() + 1);
        buf.push(Request::CONTAINS);
        buf.extend_from_slice(serkey);
        buf
    }

    #[inline]
    pub fn ser_test<V: AsRef<[u8]>>(key: V) -> Vec<u8> {
        let serkey = key.as_ref();
        let mut buf = Vec::with_capacity(serkey.len() + 1);
        buf.push(Request::TEST);
        buf.extend_from_slice(serkey);
        buf
    }

    #[inline(always)]
    pub fn ser_batch_get<'t, V: AsRef<[u8]> + 't>(values: &[V]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ipcserde::get_buffer_size_for_iter(values.iter()) + 1);
        buf.push(Request::BATCH_GET);
        ipcserde::serialize_iter_to_buffer(values.iter(), &mut buf);
        buf
    }
}
