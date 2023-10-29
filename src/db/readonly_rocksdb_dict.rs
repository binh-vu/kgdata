use std::borrow::Borrow;
use std::marker::PhantomData;

use crate::error::KGDataError;

use super::Dict;

pub struct ReadonlyRocksDBDict<K: AsRef<[u8]> + 'static, V: 'static> {
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

impl<K: AsRef<[u8]>, V: Send + Sync> Dict<K, V> for ReadonlyRocksDBDict<K, V> {
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        match self.db.get_pinned(key.as_ref())? {
            None => Ok(None),
            Some(value) => (self.deser_value)(value.as_ref()).map(|v| Some(v)),
        }
    }

    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        Ok(self.db.get_pinned(key.as_ref())?.is_some())
    }
}
