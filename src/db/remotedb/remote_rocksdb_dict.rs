use super::super::interface::Equivalent;
use super::Client;
use super::Map;
use super::Request;
use super::Response;
use hashbrown::HashMap;
use kgdata::error::KGDataError;
use rayon::prelude::*;
use std::cmp::Eq;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct BaseRemoteRocksDBDict<K: AsRef<[u8]> + Eq + Hash, V, S: Client> {
    pub sockets: Vec<S>,
    counter: AtomicUsize,
    batch_size: usize,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]> + Eq + Hash, V, S: Client> BaseRemoteRocksDBDict<K, V, S> {
    pub fn new<Q>(
        urls: &[Q],
        batch_size: usize,
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
            counter: AtomicUsize::new(0),
            batch_size,
            deser_value,
            deser_key: PhantomData,
        })
    }

    pub fn test(&self, query: &str, rotate_no: usize) -> Result<u32, KGDataError> {
        let socket = &self.sockets[rotate_no % self.sockets.len()];
        let msg = socket.request(&Request::ser_test(query))?;
        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessTest(score) => Ok(score),
            _ => Err(KGDataError::IPCImplError(
                "Invalid message. Please report the bug.".to_owned(),
            )
            .into()),
        }
    }

    #[inline(always)]
    fn get_rotate_no(&self) -> usize {
        // https://github.com/rust-lang/rust/issues/34618: C++ atomic does wrapping for
        // overflow/underflow. However, even if not, we have to do do 5M requests/second
        // for a hundred year to overflow 64-bits -- beside this remote db should only be
        // used for system that have throttling.
        self.counter.fetch_add(1, Ordering::Relaxed)
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
        let msg = socket.request(&Request::ser_batch_get(keys))?;
        let map_func = |item: &[u8]| {
            if item.len() == 0 {
                // key does not exist in the primary
                Ok(None)
            } else {
                (self.deser_value)(item).map(|v| Some(v))
            }
        };

        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessBatchGet(items) => items
                .into_iter()
                .map(map_func)
                .collect::<Result<Vec<_>, _>>(),
            Response::SuccessCompressedBatchGet(items) => items
                .into_iter()
                .map(map_func)
                .collect::<Result<Vec<_>, _>>(),
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
        let msg = socket.request(&Request::ser_batch_get(keys))?;
        let map_func = |item: &[u8]| {
            if item.len() == 0 {
                // key does not exist in the primary
                Err(KGDataError::KeyError("Key not found".to_owned()))
            } else {
                (self.deser_value)(item)
            }
        };
        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessBatchGet(items) => items
                .into_iter()
                .map(map_func)
                .collect::<Result<Vec<_>, _>>(),
            Response::SuccessCompressedBatchGet(items) => items
                .into_iter()
                .map(map_func)
                .collect::<Result<Vec<_>, _>>(),
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
        let msg = socket.request(&Request::ser_batch_get(keys))?;
        let map_func = |(item, key): (&[u8], &Q)| {
            if item.len() == 0 {
                // key does not exist in the db
                Err(KGDataError::KeyError("Key not found".to_owned()))
            } else {
                Ok((key.clone().into(), (self.deser_value)(item)?))
            }
        };
        match Response::deserialize(&msg)? {
            Response::Error => {
                Err(KGDataError::IPCImplError("Remote DB encounters an error".to_owned()).into())
            }
            Response::SuccessBatchGet(items) => items
                .into_iter()
                .zip(keys.into_iter())
                .map(map_func)
                .collect::<Result<HashMap<_, _>, _>>(),
            Response::SuccessCompressedBatchGet(items) => items
                .into_iter()
                .zip(keys.into_iter())
                .map(map_func)
                .collect::<Result<HashMap<_, _>, _>>(),
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
        let msg = socket.request(&Request::ser_get(k))?;

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
        let msg = socket.request(&Request::ser_contains(k))?;

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
        self.rotate_get(key, self.get_rotate_no())
    }

    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_contains_key(key, self.get_rotate_no())
    }

    fn slice_get<Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_batch_get(keys, self.get_rotate_no())
    }

    fn slice_get_exist<Q>(&self, keys: &[Q]) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        self.rotate_batch_get_exist(keys, self.get_rotate_no())
    }

    fn slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Into<K> + Equivalent<K> + Clone,
    {
        self.rotate_batch_get_exist_as_map(keys, self.get_rotate_no())
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
        self.rotate_batch_get_exist(&keys.into_iter().collect::<Vec<_>>(), self.get_rotate_no())
    }

    fn iter_get_exist_as_map<I: IntoIterator<Item = Q>, Q>(
        &self,
        keys: I,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K> + Into<K> + Clone,
    {
        self.rotate_batch_get_exist_as_map(
            &keys.into_iter().collect::<Vec<_>>(),
            self.get_rotate_no(),
        )
    }

    /// Get multiple keys in parallel
    fn par_slice_get<Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Sync + Send + Equivalent<K>,
    {
        keys.par_chunks(self.batch_size)
            .flat_map(
                |keys| match self.rotate_batch_get(keys, self.get_rotate_no()) {
                    Ok(lst) => lst.into_iter().map(|v| Ok(v)).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                },
            )
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist<Q>(&self, keys: &[Q]) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Sync + Send + Equivalent<K>,
    {
        keys.par_chunks(self.batch_size)
            .flat_map(
                |keys| match self.rotate_batch_get_exist(keys, self.get_rotate_no()) {
                    Ok(lst) => lst.into_iter().map(|v| Ok(v)).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                },
            )
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        K: Sync + Send,
        Q: AsRef<[u8]> + Into<K> + Sync + Send + Equivalent<K> + Clone,
    {
        keys.par_chunks(self.batch_size)
            .flat_map(
                |keys| match self.rotate_batch_get_exist(&keys, self.get_rotate_no()) {
                    Ok(lst) => lst
                        .into_iter()
                        .zip(keys)
                        .map(|(v, key)| Ok((key.clone().into(), v)))
                        .collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                },
            )
            .collect::<Result<HashMap<_, _>, KGDataError>>()
    }
}
