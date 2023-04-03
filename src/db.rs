use std::ffi::OsStr;
use std::marker::PhantomData;

use crate::conversions::WDEntityMetadata;
use crate::models::{Entity, EntityMetadata};
use crate::{conversions::WDEntity, error::KGDataError};
use rocksdb::{DBCompressionType, Options};
use serde_json;

pub struct ReadonlyRocksDBDict<K: AsRef<[u8]> + 'static, V: 'static> {
    db: rocksdb::DB,
    deser_value: Box<dyn Fn(&[u8]) -> Result<V, KGDataError> + Send + Sync>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]>, V> ReadonlyRocksDBDict<K, V> {
    pub fn get(&self, key: &K) -> Result<Option<V>, KGDataError> {
        match self.db.get_pinned(key.as_ref())? {
            None => Ok(None),
            Some(value) => (self.deser_value)(value.as_ref()).map(|v| Some(v)),
        }
    }

    pub fn contains_key(&self, key: &K) -> Result<bool, KGDataError> {
        Ok(self.db.get_pinned(key.as_ref())?.is_some())
    }
}

pub fn open_entity_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Entity>, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Zstd);
    options.set_compression_options(
        -14,       // window_bits
        6,         // level
        0,         // strategy
        16 * 1024, // max_dict_bytes
    );
    options.set_zstd_max_train_bytes(100 * 16 * 1024);

    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;
    Ok(ReadonlyRocksDBDict {
        db,
        deser_value: Box::new(deser_entity),
        deser_key: PhantomData,
    })
}

pub fn open_entity_metadata_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityMetadata>, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Zstd);
    options.set_compression_options(
        -14,       // window_bits
        6,         // level
        0,         // strategy
        16 * 1024, // max_dict_bytes
    );
    options.set_zstd_max_train_bytes(100 * 16 * 1024);

    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;
    Ok(ReadonlyRocksDBDict {
        db,
        deser_value: Box::new(deser_entity_metadata),
        deser_key: PhantomData,
    })
}

fn deser_entity(v: &[u8]) -> Result<Entity, KGDataError> {
    Ok(serde_json::from_slice::<WDEntity>(v)?.0)
}

fn deser_entity_metadata(v: &[u8]) -> Result<EntityMetadata, KGDataError> {
    Ok(serde_json::from_slice::<WDEntityMetadata>(v)?.0)
}
