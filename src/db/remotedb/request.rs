use crate::error::KGDataError;

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
            Request::BATCH_GET => Ok(Self::BatchGet(deserialize_bytes(buf))),
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
            Self::BatchGet(keys) => serialize_bytes(Request::BATCH_GET, keys),
            Self::Contains(key) => {
                let mut buf = Vec::with_capacity(key.len() + 1);
                buf.push(Request::CONTAINS);
                buf.extend_from_slice(key);
                buf
            }
        }
    }
}

#[inline(always)]
pub fn serialize_bytes<V: std::ops::Deref<Target = [u8]>>(code: u8, lst: &[V]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(lst.iter().map(|item| item.len() + 4).sum::<usize>() + 5);
    buf.push(code);
    buf.extend_from_slice(&(lst.len() as u32).to_le_bytes());
    for item in lst {
        buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
        buf.extend_from_slice(item);
    }
    buf
}

#[inline(always)]
pub fn serialize_optional_bytes<V: std::ops::Deref<Target = [u8]>>(
    code: u8,
    lst: &[Option<V>],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        lst.iter()
            .map(|item| match item {
                None => 4,
                Some(x) => x.len() + 4,
            })
            .sum::<usize>()
            + 5,
    );
    buf.push(code);
    buf.extend_from_slice(&(lst.len() as u32).to_le_bytes());
    for item in lst {
        match item {
            None => buf.extend_from_slice(&(0 as u32).to_le_bytes()),
            Some(item) => {
                buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
                buf.extend_from_slice(item);
            }
        }
    }
    buf
}

#[inline(always)]
pub fn deserialize_bytes<'t>(buf: &'t [u8]) -> Vec<&'t [u8]> {
    let n_items = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(n_items);
    let mut start = 5;
    for _i in 0..n_items {
        let size = u32::from_le_bytes(buf[start..(start + 4)].try_into().unwrap()) as usize;
        start += 4;
        out.push(&buf[start..(start + size)]);
        start += size;
    }
    out
}
