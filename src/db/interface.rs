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
    fn batch_get<'t, I, Q: ?Sized>(&self, keys: I) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + 't,
        I: IntoIterator<Item = &'t Q> + 't,
    {
        keys.into_iter()
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_batch_get<'t, I, Q: ?Sized>(
        &self,
        keys: I,
        batch_size: usize,
    ) -> Result<Vec<Option<V>>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]> + 't,
        I: IntoIterator<Item = &'t Q> + 't,
    {
        keys.into_iter()
            .chunks(batch_size)
            .into_par_iter()
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }
}
