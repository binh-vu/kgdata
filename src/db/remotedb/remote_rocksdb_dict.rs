use ahash::AHasher;
use hashbrown::HashMap;
use kgdata::error::KGDataError;
use rayon::prelude::*;
use std::cmp::Eq;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;

use super::super::interface::Equivalent;
use super::Client;
use super::Map;
use super::Request;
use super::Response;

pub struct BaseRemoteRocksDBDict<K: AsRef<[u8]> + Eq + Hash, V, S: Client> {
    sockets: Vec<S>,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]> + Eq + Hash, V, S: Client> BaseRemoteRocksDBDict<K, V, S> {
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
        Q: AsRef<[u8]> + Equivalent<K>,
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
    fn rotate_batch_get_exist<Q>(&self, keys: &[Q], rotate_no: usize) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
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
                            Err(KGDataError::KeyError("Key not found".to_owned()))
                        } else {
                            (self.deser_value)(item)
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
    fn rotate_batch_get_exist_as_map<Q>(
        &self,
        keys: &[Q],
        rotate_no: usize,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Into<K> + Equivalent<K> + Clone,
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
                    .zip(keys.into_iter())
                    .map(|(item, key)| {
                        if item.len() == 0 {
                            // key does not exist in the db
                            Err(KGDataError::KeyError("Key not found".to_owned()))
                        } else {
                            Ok((key.clone().into(), (self.deser_value)(item)?))
                        }
                    })
                    .collect::<Result<HashMap<_, _>, _>>()
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
        Q: AsRef<[u8]> + Equivalent<K>,
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
        Q: AsRef<[u8]> + Equivalent<K>,
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

impl<K: AsRef<[u8]> + Eq + Hash, V: Send + Sync, S: Client> Map<K, V>
    for BaseRemoteRocksDBDict<K, V, S>
{
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_get(key, find_worker(key.as_ref()))
    }

    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_contains_key(key, find_worker(key.as_ref()))
    }

    fn slice_get<Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_batch_get(keys, 0)
    }

    fn slice_get_exist<Q>(&self, keys: &[Q]) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_batch_get_exist(keys, 0)
    }

    fn slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Into<K> + Equivalent<K> + Clone,
    {
        self.rotate_batch_get_exist_as_map(keys, 0)
    }

    fn iter_get<I: IntoIterator<Item = Q>, Q>(&self, keys: I) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        // TODO: make it more efficient
        self.rotate_batch_get(&keys.into_iter().collect::<Vec<_>>(), 0)
    }

    fn iter_get_exist<I: IntoIterator<Item = Q>, Q>(&self, keys: I) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_batch_get_exist(&keys.into_iter().collect::<Vec<_>>(), 0)
    }

    fn iter_get_exist_as_map<I: IntoIterator<Item = Q>, Q>(
        &self,
        keys: I,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K> + Into<K> + Clone,
    {
        self.rotate_batch_get_exist_as_map(&keys.into_iter().collect::<Vec<_>>(), 0)
    }

    /// Get multiple keys in parallel
    fn par_slice_get<Q>(&self, keys: &[Q], batch_size: usize) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Sync + Send + Equivalent<K>,
    {
        keys.par_chunks(batch_size)
            .enumerate()
            .flat_map(|(i, keys)| match self.rotate_batch_get(keys, i) {
                Ok(lst) => lst.into_iter().map(|v| Ok(v)).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist<Q>(&self, keys: &[Q], batch_size: usize) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Sync + Send + Equivalent<K>,
    {
        keys.par_chunks(batch_size)
            .enumerate()
            .flat_map(|(i, keys)| match self.rotate_batch_get_exist(keys, i) {
                Ok(lst) => lst.into_iter().map(|v| Ok(v)).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist_as_map<Q>(
        &self,
        keys: &[Q],
        batch_size: usize,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        K: Sync + Send,
        Q: AsRef<[u8]> + Into<K> + Sync + Send + Equivalent<K> + Clone,
    {
        keys.par_chunks(batch_size)
            .enumerate()
            .flat_map(|(i, keys)| match self.rotate_batch_get_exist(&keys, i) {
                Ok(lst) => lst
                    .into_iter()
                    .zip(keys)
                    .map(|(v, key)| Ok((key.clone().into(), v)))
                    .collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            })
            .collect::<Result<HashMap<_, _>, KGDataError>>()
    }
}

/// Find the worker that is responsible for the given key.
#[inline]
fn find_worker(k: &[u8]) -> usize {
    let mut hasher = AHasher::default();
    hasher.write(k);
    hasher.finish() as usize
}
