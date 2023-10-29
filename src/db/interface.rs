use std::borrow::Borrow;

use crate::error::KGDataError;
use rayon::prelude::*;

/// A persistent key-value store
pub trait Dict<K: AsRef<[u8]>, V: Send + Sync>: Send + Sync {
    /// Get a key
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>;

    /// Check if a key exists
    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>;

    /// Get multiple keys
    fn batch_get<'t, Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + 't,
    {
        keys.into_iter()
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    // fn batch_get_as_map<'t, I, Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    // where
    //     K: Borrow<Q>,
    //     Q: AsRef<[u8]> + 't,
    // {
    //     keys.into_iter()
    //         .map(|key| self.get(key))
    //         .filter_map(|key| match self.get(key)? {
    //             None => Ok(None),
    //             Some(value) => Some(Ok((key, value))),
    //         })
    //         .collect::<Result<Vec<_>, KGDataError>>()
    // }

    /// Get multiple keys in parallel
    fn par_batch_get<Q>(&self, keys: &[Q], batch_size: usize) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + Sync + Send,
    {
        keys.into_par_iter()
            .with_min_len(batch_size)
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }
}
