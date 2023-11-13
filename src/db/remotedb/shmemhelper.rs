use shared_memory::{Shmem, ShmemConf};

use crate::error::KGDataError;

pub struct SharedMemBuffer {
    pub shmem: Shmem,
    pub allocator: LinearAllocator,
}

pub struct ReadonlySharedMemBuffer(pub SharedMemBuffer);

unsafe impl Send for ReadonlySharedMemBuffer {}
unsafe impl Sync for ReadonlySharedMemBuffer {}

impl SharedMemBuffer {
    pub fn get_flink(url: &str) -> String {
        assert!(
            url.starts_with("ipc://") && url.ends_with(".ipc"),
            "Cannot create flink from socket url: {}",
            url
        );
        format!("{}.flink", &url["ipc://".len()..url.len() - ".ipc".len()])
    }

    pub fn new(flink: &str, size: usize) -> Result<Self, KGDataError> {
        let shmem = match ShmemConf::new().size(size).flink(flink).create() {
            Ok(m) => m,
            Err(_) => {
                return Err(KGDataError::SharedMemoryError(
                    "Failed to create shared memory file".to_owned(),
                ));
            }
        };

        Ok(Self {
            allocator: LinearAllocator::new(flink.to_owned(), shmem.as_ptr(), size),
            shmem,
        })
    }

    pub fn open(flink: &str) -> Result<Self, KGDataError> {
        let shmem = match ShmemConf::new().flink(flink).open() {
            Ok(m) => m,
            Err(_) => {
                return Err(KGDataError::SharedMemoryError(
                    "Failed to open shared memory file".to_owned(),
                ));
            }
        };

        Ok(Self {
            allocator: LinearAllocator::new(flink.to_owned(), shmem.as_ptr(), shmem.len()),
            shmem,
        })
    }

    pub fn restore(&self, ser: &[u8]) -> AllocatedMem {
        AllocatedMem::deserialize(self.shmem.as_ptr(), ser)
    }

    pub fn alloc(&mut self, size: usize) -> Result<AllocatedMem, KGDataError> {
        self.allocator.allocate(size)
    }

    pub fn get_blocks(&self) -> Vec<AllocatedMem> {
        let mut blocks = Vec::new();
        let mut pos = 0;
        while pos < self.shmem.len() {
            let block = AllocatedMem::from_position(self.shmem.as_ptr(), pos);
            pos = block.end;
            blocks.push(block);
        }
        blocks
    }
}

pub struct AllocatedMem {
    pub mem: *mut u8,
    pub begin: usize,
    pub end: usize,
}

impl AllocatedMem {
    pub const HEADER: usize = 5;

    #[inline]
    pub fn get_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.mem.add(self.begin + AllocatedMem::HEADER),
                self.end - self.begin - AllocatedMem::HEADER,
            )
        }
    }

    #[inline]
    pub fn get_slice_mut(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.mem.add(self.begin + AllocatedMem::HEADER),
                self.end - self.begin - AllocatedMem::HEADER,
            )
        }
    }

    #[inline]
    pub fn alloc(mem: *mut u8, begin: usize, end: usize) -> Result<AllocatedMem, KGDataError> {
        if begin + AllocatedMem::HEADER > end {
            return Err(KGDataError::SharedMemoryError(format!(
                "Cannot allocate a block of memory with size less than {} bytes",
                AllocatedMem::HEADER
            )));
        }
        AllocatedMem::init(mem, 1, begin, end);
        Ok(Self { begin, end, mem })
    }

    #[inline]
    pub fn init(mem: *mut u8, is_occupied: u8, begin: usize, end: usize) {
        let usable_space = ((end - begin - AllocatedMem::HEADER) as u32).to_le_bytes();
        unsafe {
            mem.add(begin).write(is_occupied);
            mem.add(begin + 1)
                .copy_from_nonoverlapping(&usable_space as *const u8, 4);
        }
    }

    #[inline]
    pub fn is_free(mem: *mut u8, pos: usize) -> bool {
        unsafe { mem.add(pos).read() == 0 }
    }

    #[inline]
    pub fn free(&mut self) {
        unsafe {
            self.mem.add(self.begin).write(0);
        }
    }

    #[inline]
    pub fn get_allocated_size(mem: *mut u8, pos: usize) -> usize {
        // the first byte is the flag, the next 4 bytes is the size
        let blocksize: u32 = u32::from_le_bytes(
            unsafe { std::slice::from_raw_parts(mem.add(pos + 1), 4) }
                .try_into()
                .unwrap(),
        );
        blocksize as usize + 5
    }

    pub fn serialize(&self) -> [u8; 4] {
        (self.begin as u32).to_le_bytes()
    }

    pub fn deserialize(mem: *mut u8, value: &[u8]) -> AllocatedMem {
        let begin = u32::from_le_bytes(value.try_into().unwrap()) as usize;
        Self {
            mem,
            begin,
            end: begin + AllocatedMem::get_allocated_size(mem, begin),
        }
    }

    pub fn from_position(mem: *mut u8, begin: usize) -> AllocatedMem {
        Self {
            mem,
            begin,
            end: begin + AllocatedMem::get_allocated_size(mem, begin),
        }
    }
}

pub struct LinearAllocator {
    id: String,
    mem: *mut u8,
    size: usize,
    begin: usize,
    end: usize,
    // the real end that if we pass this value, the remaining space is too small to store anything
    // it is end - AllocatedMem::HEADER (so at least it can store the header and be a valid block)
    usable_end: usize,
}

impl LinearAllocator {
    pub const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1);

    pub fn new(id: String, mem: *mut u8, size: usize) -> Self {
        AllocatedMem::init(mem, 0, 0, size);
        Self {
            id,
            mem,
            size,
            begin: 0,
            end: size,
            usable_end: size - AllocatedMem::HEADER,
        }
    }

    pub fn allocate(&mut self, size: usize) -> Result<AllocatedMem, KGDataError> {
        let actual_size = size + AllocatedMem::HEADER;
        if actual_size > self.size {
            return Err(KGDataError::SharedMemoryError(
                format!(
                    "Cannot allocate more memory than what have been assigned (request {} bytes but only have {} bytes)",
                    size, self.size),
            ));
        }
        self._alloc(actual_size)
    }

    fn _alloc(&mut self, size: usize) -> Result<AllocatedMem, KGDataError> {
        // try to allocate at the end of the buffer
        while size > self.end - self.begin && self.end < self.size {
            self.end += self.try_free(self.end)?;
        }
        if size < self.end - self.begin {
            self.begin += size;
            if self.begin < self.usable_end {
                // if the remaining space is too small, we cannot mark it as free, the try_free function
                // will take care of this.
                AllocatedMem::init(self.mem, 0, self.begin, self.end);
            }
            return AllocatedMem::alloc(self.mem, self.begin - size, self.begin);
        } else {
            // the remaining space is not enough -- but we should update the current one with how much we have claimed
            AllocatedMem::init(self.mem, 0, self.begin, self.end);
        }

        // we do not have enough space at the end of the buffer, so we have to start from beginning
        self.begin = 0;
        self.end = 0;
        while let Some(freed) = self.try_free_nonblocking(self.end) {
            self.end += freed;
            if self.end >= self.size {
                break;
            }
        }
        self._alloc(size)
    }

    fn try_free(&self, pos: usize) -> Result<usize, KGDataError> {
        if pos >= self.usable_end {
            return Ok(self.end - pos);
        }

        if !AllocatedMem::is_free(self.mem, pos) {
            // wait at most 5 seconds
            for _ in 0..5000 {
                if AllocatedMem::is_free(self.mem, pos) {
                    break;
                }
                std::thread::sleep(LinearAllocator::CHECK_INTERVAL);
            }
            if !AllocatedMem::is_free(self.mem, pos) {
                return Err(KGDataError::SharedMemoryError(
                    format!(
                        "Encounter possible deadlock situation as we have wait for more than 5s for a free block of shared memory at {}:{}",
                        &self.id, pos,
                    ),
                ));
            }
        }

        Ok(AllocatedMem::get_allocated_size(self.mem, pos))
    }

    fn try_free_nonblocking(&self, pos: usize) -> Option<usize> {
        if pos >= self.usable_end {
            return Some(self.usable_end - pos);
        }

        if AllocatedMem::is_free(self.mem, pos) {
            Some(AllocatedMem::get_allocated_size(self.mem, pos))
        } else {
            None
        }
    }
}
