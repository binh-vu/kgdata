pub trait Buffer {
    fn write_byte(&mut self, byte: u8);
    fn write(&mut self, content: &[u8]);
}

pub struct VecBuffer(pub Vec<u8>);

impl VecBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl Buffer for VecBuffer {
    fn write_byte(&mut self, byte: u8) {
        self.0.push(byte);
    }

    fn write(&mut self, content: &[u8]) {
        self.0.extend_from_slice(content);
    }
}

impl Buffer for Vec<u8> {
    fn write_byte(&mut self, byte: u8) {
        self.push(byte);
    }

    fn write(&mut self, content: &[u8]) {
        self.extend_from_slice(content);
    }
}

#[inline(always)]
pub fn serialize_lst<V: std::ops::Deref<Target = [u8]>>(code: u8, lst: &[V]) -> Vec<u8> {
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
pub fn serialize_lst_to_buffer<V: std::ops::Deref<Target = [u8]>>(
    code: u8,
    lst: &[V],
    buf: &mut impl Buffer,
) -> usize {
    let mut size = 5;
    buf.write_byte(code);
    buf.write(&(lst.len() as u32).to_le_bytes());
    for item in lst {
        buf.write(&(item.len() as u32).to_le_bytes());
        buf.write(item);
        size += 4 + item.len();
    }
    size
}

#[allow(dead_code)]
#[inline(always)]
pub fn serialize_optional_lst<V: std::ops::Deref<Target = [u8]>>(
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
pub fn serialize_optional_lst_to_buffer<V: std::ops::Deref<Target = [u8]>>(
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
