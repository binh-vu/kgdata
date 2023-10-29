use ahash::AHasher;
use kgdata::error::KGDataError;
use rayon::prelude::*;
use std::borrow::Borrow;
use std::hash::Hasher;
use std::marker::PhantomData;

use super::Dict;

pub mod ipcdeser;
pub mod nngserver;
pub mod request;
pub mod response;
pub use self::nngserver::{serve_db, NNGClient, NNGLocalClient};
pub use self::request::Request;
pub use self::response::Response;

pub trait Client: Send + Sync {
    type Message: std::ops::Deref<Target = [u8]>;

    fn open(url: &str) -> Result<Self, KGDataError>
    where
        Self: Sized;

    // send a request to the server
    fn request(&self, req: &[u8]) -> Result<Self::Message, KGDataError>;
}

pub struct BaseRemoteRocksDBDict<K: AsRef<[u8]> + 'static, V: 'static, S: Client> {
    sockets: Vec<S>,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]>, V, S: Client> BaseRemoteRocksDBDict<K, V, S> {
    pub fn new<Q>(
        urls: &[Q],
        deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    ) -> Result<Self, KGDataError>
    where
        Q: AsRef<str>,
    {
        let sockets = urls
            .into_iter()
            .map(|url| S::open(url.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            sockets,
            deser_value,
            deser_key: PhantomData,
        })
    }

    #[inline(always)]
    fn rotate_batch_get<Q>(
        &self,
        keys: &[Q],
        rotate_no: usize,
    ) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let socket = &self.sockets[rotate_no % self.sockets.len()];
        let msg = socket.request(&Request::serialize_batch_get(
            keys.iter().map(|key| key.as_ref()),
            keys.len(),
            keys.iter().map(|key| key.as_ref().len()).sum::<usize>(),
        ))?;

        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessBatchGet(items) => {
                items
                    .into_iter()
                    .map(|item| {
                        if item.len() == 0 {
                            // key does not exist in the primary
                            Ok(None)
                        } else {
                            (self.deser_value)(item).map(|v| Some(v))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline(always)]
    fn ref_rotate_batch_get<Q>(
        &self,
        keys: &[&Q],
        rotate_no: usize,
    ) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let socket = &self.sockets[rotate_no % self.sockets.len()];
        let msg = socket.request(&Request::serialize_batch_get(
            keys.iter().map(|key| key.as_ref()),
            keys.len(),
            keys.iter().map(|key| key.as_ref().len()).sum::<usize>(),
        ))?;

        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessBatchGet(items) => {
                items
                    .into_iter()
                    .map(|item| {
                        if item.len() == 0 {
                            // key does not exist in the primary
                            Ok(None)
                        } else {
                            (self.deser_value)(item).map(|v| Some(v))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline(always)]
    fn rotate_get<Q: ?Sized>(&self, key: &Q, rotate_no: usize) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let k = key.as_ref();
        let socket = &self.sockets[rotate_no % self.sockets.len()];
        let msg = socket.request(&Request::Get(k).serialize())?;

        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessGet(data) => {
                if data.len() == 0 {
                    // key does not exist in the primary
                    Ok(None)
                } else {
                    (self.deser_value)(data).map(|v| Some(v))
                }
            }
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline(always)]
    fn rotate_contains_key<Q: ?Sized>(&self, key: &Q, rotate_no: usize) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        let k = key.as_ref();
        let socket = &self.sockets[rotate_no % self.sockets.len()];
        let msg = socket.request(&Request::Contains(k).serialize())?;

        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessContains(b) => Ok(b),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }
}

impl<K: AsRef<[u8]>, V: Send + Sync, S: Client> Dict<K, V> for BaseRemoteRocksDBDict<K, V, S> {
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        self.rotate_get(key, find_worker(key.as_ref()))
    }

    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        self.rotate_contains_key(key, find_worker(key.as_ref()))
    }

    fn batch_get<'t, Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + 't,
    {
        self.rotate_batch_get(keys, 0)
    }

    /// Get multiple keys in parallel
    fn par_batch_get<Q>(&self, keys: &[Q], batch_size: usize) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + Sync + Send,
    {
        keys.into_par_iter()
            .chunks(batch_size)
            .enumerate()
            .flat_map(
                |(i, keys)| match self.ref_rotate_batch_get(keys.as_ref(), i) {
                    Ok(lst) => lst.into_iter().map(|v| Ok(v)).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                },
            )
            .collect::<Result<Vec<_>, KGDataError>>()
    }
}

pub type RemoteRocksDBDict<K, V> = BaseRemoteRocksDBDict<K, V, NNGLocalClient>;

/// Find the worker that is responsible for the given key.
#[inline]
fn find_worker(k: &[u8]) -> usize {
    let mut hasher = AHasher::default();
    hasher.write(k);
    hasher.finish() as usize
}
