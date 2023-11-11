use std::ops::Deref;

use crate::error::KGDataError;

use super::{
    ipcserde::{self, Buffer},
    shmemhelper::SharedMemBuffer,
};

#[derive(Debug)]
pub enum Response<'s> {
    Error,
    // contains value, empty if there is no value
    SuccessGet(&'s [u8]),
    // contains values, for each item, it's empty if there is no value
    SuccessBatchGet(Vec<&'s [u8]>),
    // compressed version of batch get
    SuccessCompressedBatchGet(CompressedBatchGet),
    SuccessShmBatchGet(&'s [u8]),
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
    pub const SUCCESS_SHM_BATCH_GET: u8 = 5;
    pub const SUCCESS_CONTAINS: u8 = 4;
    pub const SUCCESS_TEST: u8 = 10;

    pub fn deserialize(buf: &'s [u8]) -> Result<Self, KGDataError> {
        match buf[0] {
            Response::ERROR => Ok(Self::Error),
            Response::SUCCESS_GET => Ok(Self::SuccessGet(&buf[1..])),
            Response::SUCCESS_BATCH_GET => {
                Ok(Self::SuccessBatchGet(ipcserde::deserialize_lst(&buf[1..])))
            }
            Response::SUCCESS_COMPRESSED_BATCH_GET => {
                let uncompressed_stream = zstd::stream::decode_all(&buf[1..])?;
                Ok(Self::SuccessCompressedBatchGet(CompressedBatchGet {
                    items: ipcserde::deserialize_lst_range(&uncompressed_stream),
                    uncompressed_stream,
                }))
            }
            Response::SUCCESS_CONTAINS => Ok(Self::SuccessContains(buf[1] == 1)),
            Response::SUCCESS_SHM_BATCH_GET => Ok(Self::SuccessShmBatchGet(&buf[1..])),
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
    pub fn ser_error(buf: &mut impl ipcserde::Buffer) -> usize {
        buf.write_byte(Response::ERROR);
        1
    }

    #[inline]
    pub fn ser_success_get<V: Deref<Target = [u8]>>(
        value: V,
        buf: &mut impl ipcserde::Buffer,
    ) -> usize {
        buf.write_byte(Response::SUCCESS_GET);
        buf.write(&value);
        1 + value.len()
    }

    #[inline]
    pub fn ser_success_contains(value: bool, buf: &mut impl ipcserde::Buffer) -> usize {
        buf.write(&[Response::SUCCESS_CONTAINS, value as u8]);
        2
    }

    #[inline]
    pub fn ser_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: impl Iterator<Item = V> + ExactSizeIterator,
        buf: &mut impl ipcserde::Buffer,
    ) -> usize {
        buf.write_byte(Response::SUCCESS_BATCH_GET);
        ipcserde::serialize_iter_to_buffer(values, buf) + 1
    }

    #[inline]
    pub fn ser_compressed_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: impl Iterator<Item = V> + ExactSizeIterator,
        buf: &mut ipcserde::VecBuffer,
    ) -> usize {
        buf.write_byte(Response::SUCCESS_COMPRESSED_BATCH_GET);
        ipcserde::compressed_serialize_iter_to_buffer(values, buf) + 1
    }

    /// Serialize the data and put it into shared memory.
    #[inline]
    pub fn ser_shm_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: &[V],
        buf: &mut ipcserde::VecBuffer,
        shm: &mut SharedMemBuffer,
    ) -> Result<usize, KGDataError> {
        buf.write_byte(Response::SUCCESS_SHM_BATCH_GET);
        let size = ipcserde::get_buffer_size_for_iter(values.iter());
        let shmbuf = shm.alloc(size)?;
        ipcserde::serialize_iter_to_buffer(
            values.iter(),
            &mut ipcserde::SliceBuffer::new(shmbuf.get_slice_mut()),
        );
        let sershmbuf = shmbuf.serialize();
        buf.write(&sershmbuf);
        Ok(1 + sershmbuf.len())
    }

    /// Serialize the data (compressed) and put it into shared memory
    #[inline]
    pub fn ser_compressed_shm_success_batch_get<'t, V: AsRef<[u8]> + 't>(
        values: &[V],
        buf: &mut ipcserde::VecBuffer,
        shm: &mut SharedMemBuffer,
    ) -> Result<usize, KGDataError> {
        buf.write_byte(Response::SUCCESS_SHM_BATCH_GET);
        let size = 1024 * 1024; // 1MB
        let shmbuf = shm.alloc(size)?;
        let len = ipcserde::compressed_serialize_iter_to_buffer_2(
            values.iter(),
            &mut ipcserde::SliceBuffer::new(&mut shmbuf.get_slice_mut()[4..]),
        );
        assert!(len + 4 <= size);
        (&mut shmbuf.get_slice_mut()[..4]).copy_from_slice(&(len as u32).to_le_bytes());
        let sershmbuf = shmbuf.serialize();
        buf.write(&sershmbuf);
        Ok(1 + sershmbuf.len())
    }

    #[inline]
    pub fn deser_compressed_shm_success_batch_get(
        shm: &mut super::shmemhelper::ReadonlySharedMemBuffer,
        ser: &[u8],
    ) -> Result<CompressedBatchGet, KGDataError> {
        let mut allocmem = shm.0.restore(ser);
        let allocmemslice = allocmem.get_slice();

        // let res = ipcserde::deserialize_lst(allocmemslice)
        //     .into_iter()
        //     .map(map_func)
        //     .collect::<Result<Vec<_>, _>>();
        let len = u32::from_le_bytes(allocmemslice[..4].try_into().unwrap()) as usize;
        let uncompressed_stream = zstd::stream::decode_all(&allocmemslice[4..len + 4])?;
        let res = super::response::CompressedBatchGet {
            items: ipcserde::deserialize_lst_range(&uncompressed_stream),
            uncompressed_stream,
        };

        allocmem.free();
        Ok(res)
    }

    #[inline]
    pub fn ser_success_test(value: u32, buf: &mut impl ipcserde::Buffer) -> usize {
        buf.write_byte(Response::SUCCESS_TEST);
        buf.write(&value.to_le_bytes());
        5
    }
}

#[derive(Debug)]
pub struct CompressedBatchGet {
    pub uncompressed_stream: Vec<u8>,
    pub items: Vec<(usize, usize)>,
}

impl CompressedBatchGet {
    // Iterate over each item in the uncompressed stream. This function should
    // be named iter(), but instead name into_iter() for the sake of written match
    // arm easier. We should not use it outside of match arm.
    pub fn into_iter(&self) -> impl Iterator<Item = &[u8]> {
        self.items
            .iter()
            .map(|(start, end)| &self.uncompressed_stream[*start..*end])
    }
}
