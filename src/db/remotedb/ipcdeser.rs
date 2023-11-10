use std::io::Write;
use std::ops::Deref;

use rocksdb::DBPinnableSlice;

pub struct EmptySlice;

impl Deref for EmptySlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &[]
    }
}

pub struct OptionDBPinnableSlice<'s>(pub Option<DBPinnableSlice<'s>>);

impl<'s> Deref for OptionDBPinnableSlice<'s> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            None => &[],
            Some(x) => x.deref(),
        }
    }
}

impl<'s> AsRef<[u8]> for OptionDBPinnableSlice<'s> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            None => &[],
            Some(x) => x.as_ref(),
        }
    }
}

pub trait Buffer {
    fn write_byte(&mut self, byte: u8);
    fn write(&mut self, content: &[u8]);
    fn write_at(&mut self, content: &[u8], at: usize);
}

pub struct VecBuffer(pub Vec<u8>);
pub struct RefVecBuffer<'s>(pub &'s mut Vec<u8>);

impl VecBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn get_mut_ref(&mut self) -> RefVecBuffer {
        RefVecBuffer(&mut self.0)
    }
}

impl std::io::Write for VecBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'s> std::io::Write for RefVecBuffer<'s> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Buffer for VecBuffer {
    fn write_byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn write(&mut self, content: &[u8]) {
        self.0.extend_from_slice(content);
    }

    fn write_at(&mut self, content: &[u8], at: usize) {
        self.0[at..(at + content.len())].copy_from_slice(content);
    }
}

impl Buffer for Vec<u8> {
    fn write_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    fn write(&mut self, content: &[u8]) {
        self.extend_from_slice(content);
    }

    fn write_at(&mut self, content: &[u8], at: usize) {
        self[at..(at + content.len())].copy_from_slice(content);
    }
}

#[inline(always)]
pub fn serialize_lst<V: Deref<Target = [u8]>>(code: u8, lst: &[V]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(lst.iter().map(|item| item.len() + 4).sum::<usize>() + 5);
    buf.push(code);
    buf.extend_from_slice(&(lst.len() as u32).to_le_bytes());
    for item in lst {
        buf.extend_from_slice(&(item.len() as u32).to_le_bytes());
        buf.extend_from_slice(item);
    }
    buf
}

#[inline]
pub fn get_buffer_size_for_iter<'t, V: AsRef<[u8]> + 't>(
    iter: impl Iterator<Item = V> + ExactSizeIterator,
) -> usize {
    return 5 + iter.map(|item| item.as_ref().len() + 4).sum::<usize>();
}

#[inline]
pub fn serialize_iter_to_buffer<'t, V: AsRef<[u8]> + 't>(
    code: u8,
    iter: impl Iterator<Item = V> + ExactSizeIterator,
    buf: &mut impl Buffer,
) -> usize {
    let mut size = 5;
    buf.write_byte(code);
    buf.write(&(iter.len().to_le_bytes()));
    for item in iter {
        let item = item.as_ref();
        buf.write(&(item.len() as u32).to_le_bytes());
        buf.write(item);
        size += 4 + item.len();
    }
    size
}

#[inline(always)]
pub fn serialize_optional_lst_to_buffer<V: Deref<Target = [u8]>>(
    code: u8,
    lst: &[Option<V>],
    buf: &mut impl Buffer,
) -> usize {
    let mut size = 5;
    buf.write_byte(code);
    buf.write(&(lst.len() as u32).to_le_bytes());
    for item in lst {
        match item {
            None => {
                buf.write(&(0 as u32).to_le_bytes());
                size += 4;
            }
            Some(x) => {
                buf.write(&(x.len() as u32).to_le_bytes());
                buf.write(&x);
                size += 4 + x.len();
            }
        }
    }
    size
}

#[inline(always)]
pub fn serialize_compressed_optional_lst_to_buffer<V: Deref<Target = [u8]>>(
    code: u8,
    lst: &[Option<V>],
    buf: &mut VecBuffer,
) -> usize {
    let mut encoder = zstd::stream::write::Encoder::new(buf.get_mut_ref(), 3).unwrap();
    encoder.write_all(&[code]).unwrap();
    encoder
        .write_all(&(lst.len() as u32).to_le_bytes())
        .unwrap();

    for item in lst {
        match item {
            None => {
                encoder.write_all(&(0 as u32).to_le_bytes()).unwrap();
            }
            Some(x) => {
                encoder.write_all(&(x.len() as u32).to_le_bytes()).unwrap();
                encoder.write_all(&x).unwrap();
            }
        }
    }
    encoder.flush().unwrap();
    let refbuf = encoder.finish().unwrap();
    refbuf.0.len()
}

#[inline(always)]
pub fn deserialize_lst<'t>(buf: &'t [u8]) -> Vec<&'t [u8]> {
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
