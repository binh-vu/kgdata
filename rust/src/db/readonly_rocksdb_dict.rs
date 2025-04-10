use hashbrown::HashMap;

use crate::error::KGDataError;
use std::cmp::Eq;
use std::hash::Hash;
use std::marker::PhantomData;

use super::interface::Equivalent;
use super::Map;
use rayon::prelude::*;

pub struct ReadonlyRocksDBDict<K: AsRef<[u8]>, V> {
    db: rocksdb::DB,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]>, V> ReadonlyRocksDBDict<K, V> {
    pub fn new(db: rocksdb::DB, deser_value: fn(&[u8]) -> Result<V, KGDataError>) -> Self {
        Self {
            db,
            deser_value,
            deser_key: PhantomData,
        }
    }
}

impl<K: AsRef<[u8]> + Eq + Hash, V: Send + Sync> Map<K, V> for ReadonlyRocksDBDict<K, V> {
    #[inline]
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        match self.db.get_pinned(key.as_ref())? {
            None => Ok(None),
            Some(value) => (self.deser_value)(value.as_ref()).map(|v| Some(v)),
        }
        // match self.db.get(key.as_ref())? {
        //     None => Ok(None),
        //     Some(value) => (self.deser_value)(value.as_ref()).map(|v| Some(v)),
        // }
    }

    #[inline]
    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        Ok(self.db.get_pinned(key.as_ref())?.is_some())
        // Ok(self.db.get(key.as_ref())?.is_some())
    }

    fn slice_get<Q>(&self, keys: &[Q]) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        keys.into_iter()
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn slice_get_exist<Q: AsRef<[u8]> + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<V>, KGDataError> {
        keys.into_iter()
            .map(|key| match self.get(key)? {
                None => Err(KGDataError::KeyError("Key not found".to_owned())),
                Some(value) => Ok(value),
            })
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Into<K> + Equivalent<K> + Clone,
    {
        keys.into_iter()
            .map(|key| match self.get(key)? {
                None => Err(KGDataError::KeyError("Key not found".to_owned())),
                Some(value) => Ok((key.clone().into(), value)),
            })
            .collect::<Result<HashMap<_, _>, KGDataError>>()
    }

    fn iter_get<I: IntoIterator<Item = Q>, Q>(&self, keys: I) -> Result<Vec<Option<V>>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        keys.into_iter()
            .map(|key| self.get(&key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn iter_get_exist<I: IntoIterator<Item = Q>, Q>(&self, keys: I) -> Result<Vec<V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K>,
    {
        keys.into_iter()
            .map(|key| match self.get(&key)? {
                None => Err(KGDataError::KeyError("Key not found".to_owned())),
                Some(value) => Ok(value),
            })
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn iter_get_exist_as_map<I: IntoIterator<Item = Q>, Q>(
        &self,
        keys: I,
    ) -> Result<HashMap<K, V>, KGDataError>
    where
        Q: AsRef<[u8]> + Equivalent<K> + Into<K> + Clone,
    {
        keys.into_iter()
            .map(|key| match self.get(&key)? {
                None => Err(KGDataError::KeyError("Key not found".to_owned())),
                Some(value) => Ok((key.clone().into(), value)),
            })
            .collect::<Result<HashMap<_, _>, KGDataError>>()
    }

    fn par_slice_get<Q: AsRef<[u8]> + Sync + Send + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<Option<V>>, KGDataError> {
        keys.into_par_iter()
            .map(|key| self.get(key))
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist<Q: AsRef<[u8]> + Sync + Send + Equivalent<K>>(
        &self,
        keys: &[Q],
    ) -> Result<Vec<V>, KGDataError> {
        keys.into_par_iter()
            .map(|key| {
                self.get(key)?
                    .ok_or_else(|| KGDataError::KeyError("Key not found".to_owned()))
            })
            .collect::<Result<Vec<_>, KGDataError>>()
    }

    fn par_slice_get_exist_as_map<Q>(&self, keys: &[Q]) -> Result<HashMap<K, V>, KGDataError>
    where
        K: Sync + Send,
        Q: AsRef<[u8]> + Into<K> + Sync + Send + Equivalent<K> + Clone,
    {
        keys.into_par_iter()
            .map(|key| match self.get(key)? {
                None => Err(KGDataError::KeyError("Key not found".to_owned())),
                Some(value) => Ok((key.clone().into(), value)),
            })
            .collect::<Result<HashMap<_, _>, KGDataError>>()
    }
}
