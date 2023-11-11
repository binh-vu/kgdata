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

pub struct SliceBuffer<'s> {
    pub slice: &'s mut [u8],
    pub start: usize,
}

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

impl<'s> SliceBuffer<'s> {
    pub fn new(slice: &'s mut [u8]) -> Self {
        Self { slice, start: 0 }
    }
}

impl<'s> Buffer for SliceBuffer<'s> {
    fn write_byte(&mut self, byte: u8) {
        self.slice[self.start] = byte;
        self.start += 1;
    }

    fn write(&mut self, content: &[u8]) {
        self.slice[self.start..(self.start + content.len())].copy_from_slice(content);
        self.start += content.len();
    }

    fn write_at(&mut self, content: &[u8], at: usize) {
        self.slice[at..(at + content.len())].copy_from_slice(content);
    }
}

impl<'s> std::io::Write for SliceBuffer<'s> {
    fn write(&mut self, content: &[u8]) -> std::io::Result<usize> {
        self.slice[self.start..(self.start + content.len())].copy_from_slice(content);
        self.start += content.len();
        Ok(content.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[inline]
pub fn get_buffer_size_for_iter<'t, V: AsRef<[u8]> + 't>(
    iter: impl Iterator<Item = V> + ExactSizeIterator,
) -> usize {
    return 4 + iter.map(|item| item.as_ref().len() + 4).sum::<usize>();
}

#[inline]
pub fn serialize_iter_to_buffer<'t, V: AsRef<[u8]> + 't>(
    iter: impl Iterator<Item = V> + ExactSizeIterator,
    buf: &mut impl Buffer,
) -> usize {
    let mut size = 4;
    buf.write(&((iter.len() as u32).to_le_bytes()));
    for item in iter {
        let item = item.as_ref();
        buf.write(&(item.len() as u32).to_le_bytes());
        buf.write(item);
        size += 4 + item.len();
    }
    size
}

#[inline]
pub fn compressed_serialize_iter_to_buffer<'t, V: AsRef<[u8]> + 't>(
    iter: impl Iterator<Item = V> + ExactSizeIterator,
    buf: &mut VecBuffer,
) -> usize {
    let origin_len = buf.0.len();

    let mut encoder = zstd::stream::write::Encoder::new(buf.get_mut_ref(), 3).unwrap();
    encoder
        .write_all(&((iter.len() as u32).to_le_bytes()))
        .unwrap();

    for item in iter {
        let item = item.as_ref();
        encoder
            .write_all(&(item.len() as u32).to_le_bytes())
            .unwrap();
        encoder.write_all(item).unwrap();
    }

    encoder.flush().unwrap();
    encoder.finish().unwrap();

    buf.0.len() - origin_len
}

#[inline]
pub fn compressed_serialize_iter_to_buffer_2<'t, V: AsRef<[u8]> + 't>(
    iter: impl Iterator<Item = V> + ExactSizeIterator,
    buf: &mut SliceBuffer,
) -> usize {
    let origin_len = buf.start;

    let mut encoder = zstd::stream::write::Encoder::new(buf, 3).unwrap();
    encoder
        .write_all(&((iter.len() as u32).to_le_bytes()))
        .unwrap();

    for item in iter {
        let item = item.as_ref();
        encoder
            .write_all(&(item.len() as u32).to_le_bytes())
            .unwrap();
        encoder.write_all(item).unwrap();
    }

    encoder.flush().unwrap();
    encoder.finish().unwrap().start - origin_len
}

#[inline(always)]
pub fn deserialize_lst<'t>(buf: &'t [u8]) -> Vec<&'t [u8]> {
    let n_items = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(n_items);
    let mut start = 4;
    for _i in 0..n_items {
        let size = u32::from_le_bytes(buf[start..(start + 4)].try_into().unwrap()) as usize;
        start += 4;
        out.push(&buf[start..(start + size)]);
        start += size;
    }
    out
}

#[inline(always)]
pub fn deserialize_lst_range<'t>(buf: &'t [u8]) -> Vec<(usize, usize)> {
    let n_items = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    let mut out = Vec::with_capacity(n_items);
    let mut start = 4;
    for _i in 0..n_items {
        let size = u32::from_le_bytes(buf[start..(start + 4)].try_into().unwrap()) as usize;
        out.push((start + 4, start + size + 4));
        start += size + 4;
    }
    out
}
