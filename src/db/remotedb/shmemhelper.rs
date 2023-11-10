use shared_memory::{Shmem, ShmemConf};

use crate::error::KGDataError;

pub struct SharedMemBuffer {
    pub shmem: Shmem,
}

impl SharedMemBuffer {
    pub fn new(flink: &str, size: usize) -> Result<Self, KGDataError> {
        let shmem = match ShmemConf::new()
            .size(10 * 1024 * 1024)
            .flink(flink)
            .create()
        {
            Ok(m) => m,
            Err(e) => {
                return Err(KGDataError::IPCImplError(
                    "Failed to create shared memory file".to_owned(),
                ));
            }
        };

        Ok(Self { shmem })
    }

    // Find an available slot of size that we can write to
    pub fn find_slot(&mut self, size: usize) -> usize {
        todo!()
    }

    pub fn find_available_buffer(&mut self, size: usize) -> &mut [u8] {
        let slot = self.find_slot(size);
        unsafe { &mut self.shmem.as_slice_mut()[slot..slot + size] }
    }

    // Free a slot in our buffer. The slot identifier is the position of the beginning byte of the slot
    pub fn free_slot(&mut self, begin: usize) {
        todo!()
    }
}
