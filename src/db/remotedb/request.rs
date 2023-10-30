use crate::error::KGDataError;

use super::ipcdeser;

#[derive(Debug)]
pub enum Request<'s> {
    // Get a key from the database
    Get(&'s [u8]),

    // Get multiple keys from the database
    BatchGet(Vec<&'s [u8]>),

    // Check if a key exists in the database
    Contains(&'s [u8]),
}

impl<'s> Request<'s> {
    pub const GET: u8 = 0;
    pub const BATCH_GET: u8 = 1;
    pub const CONTAINS: u8 = 2;

    #[inline]
    pub fn serialize_batch_get<'t>(
        it: impl IntoIterator<Item = &'t [u8]> + 't,
        it_len: usize,
        it_size: usize,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(it_size + 4 * it_len + 5);
        buf.push(Request::BATCH_GET);
        buf.extend_from_slice(&(it_len as u32).to_le_bytes());
        for item in it {
            buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
            buf.extend_from_slice(item);
        }
        buf
    }

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Request::GET => Ok(Self::Get(&buf[1..])),
            Request::BATCH_GET => Ok(Self::BatchGet(ipcdeser::deserialize_lst(buf))),
            Request::CONTAINS => Ok(Self::Contains(&buf[1..])),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::Get(key) => {
                let mut buf = Vec::with_capacity(key.len() + 1);
                buf.push(Request::GET);
                buf.extend_from_slice(key);
                buf
            }
            Self::BatchGet(keys) => ipcdeser::serialize_lst(Request::BATCH_GET, keys),
            Self::Contains(key) => {
                let mut buf = Vec::with_capacity(key.len() + 1);
                buf.push(Request::CONTAINS);
                buf.extend_from_slice(key);
                buf
            }
        }
    }

    #[allow(dead_code)]
    pub fn serialize_to_buf(&self, buf: &mut impl ipcdeser::Buffer) -> usize {
        match self {
            Self::Get(key) => {
                buf.write_byte(Request::GET);
                buf.write(key);
                1 + key.len()
            }
            Self::BatchGet(keys) => {
                ipcdeser::serialize_lst_to_buffer(Request::BATCH_GET, keys, buf)
            }
            Self::Contains(key) => {
                buf.write_byte(Request::CONTAINS);
                buf.write(key);
                1 + key.len()
            }
        }
    }
}
