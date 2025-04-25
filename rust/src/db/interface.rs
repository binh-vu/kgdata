use crate::error::KGDataError;
use hashbrown::HashMap;
use std::cmp::Eq;
use std::hash::Hash;

/// An union interface for our RocksDB (local and remote)
pub trait Map<K: AsRef<[u8]> + Eq + Hash, V: Send + Sync>: Send + Sync {
    /// Get a key
    fn get<Q: ?Sized + AsRef<[u8]> + Equivalent<K>>(
        &self,
        key: &Q,
    ) -> Result<Option<V>, KGDataError>;

    /// Check if a key exists
    fn contains_key<Q: ?Sized + AsRef<[u8]> + Equivalent<K>>(
        &self,
        key: &Q,
    ) -> Result<bool, KGDataError>;

    /// Get multiple keys
    fn slice_get<Q: AsRef<[u8]> + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<Option<V>>, KGDataError>;

    /// Get existing keys -- error if any key does not exist
    fn slice_get_exist<Q: AsRef<[u8]> + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<V>, KGDataError>;

    /// Get existing keys as a map -- error if any key does not exist
    fn slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Into<K> + Equivalent<K> + Clone;

    fn iter_get<I: IntoIterator<Item = Q>, Q>(
        &self,
        keys: I,
    ) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>;

    fn iter_get_exist<I: IntoIterator<Item = Q>, Q>(&self, keys: I) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>;

    fn iter_get_exist_as_map<I: IntoIterator<Item = Q>, Q>(
        &self,
        keys: I,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K> + Into<K> + Clone;

    /// Get multiple keys in parallel
    fn par_slice_get<Q: AsRef<[u8]> + Sync + Send + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<Option<V>>, KGDataError>;

    fn par_slice_get_exist<Q: AsRef<[u8]> + Sync + Send + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<V>, KGDataError>;

    fn par_slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        K: Sync + Send,
        Q: AsRef<[u8]> + Into<K> + Sync + Send + Equivalent<K> + Clone;
}

pub trait Equivalent<T: ?Sized> {}

impl<T: ?Sized> Equivalent<T> for T {}

impl<T: ?Sized> Equivalent<T> for &T {}

impl<T: ?Sized> Equivalent<T> for &&T {}

impl<T: ?Sized> Equivalent<&T> for T {}

impl Equivalent<String> for &str {}

impl Equivalent<String> for str {}

// impl<'t, A: ?Sized, B: ?Sized> Equivalent<B> for A where A: Equivalent<&'t B> {}
