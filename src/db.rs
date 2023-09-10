use std::borrow::Borrow;
use std::ffi::OsStr;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::conversions::{WDClass, WDEntityMetadata, WDProperty};
use crate::models::{Class, Entity, EntityMetadata, EntityOutLink, Property};
use crate::{conversions::WDEntity, error::KGDataError};
use rocksdb::{DBCompressionType, Options};
use serde_json;

pub struct KGDB {
    pub datadir: PathBuf,
    pub classes: ReadonlyRocksDBDict<String, Class>,
    pub props: ReadonlyRocksDBDict<String, Property>,
    pub entities: ReadonlyRocksDBDict<String, Entity>,
    pub entity_metadata: ReadonlyRocksDBDict<String, EntityMetadata>,
    pub entity_outlink: ReadonlyRocksDBDict<String, EntityOutLink>,
    pub entity_pagerank: ReadonlyRocksDBDict<String, f64>,
}

impl KGDB {
    pub fn new(datadir: &str) -> Result<Self, KGDataError> {
        let datadir = PathBuf::from(datadir);
        Ok(Self {
            props: open_property_db(datadir.join("props.db").as_os_str())?,
            classes: open_class_db(datadir.join("classes.db").as_os_str())?,
            entities: open_entity_db(datadir.join("entities.db").as_os_str())?,
            entity_metadata: open_entity_metadata_db(
                datadir.join("entity_metadata.db").as_os_str(),
            )?,
            entity_outlink: open_entity_link_db(datadir.join("entity_outlinks.db").as_os_str())?,
            entity_pagerank: open_entity_pagerank_db(
                datadir.join("entity_pagerank.db").as_os_str(),
            )?,
            datadir,
        })
    }
}

pub struct ReadonlyRocksDBDict<K: AsRef<[u8]> + 'static, V: 'static> {
    db: rocksdb::DB,
    deser_value: fn(&[u8]) -> Result<V, KGDataError>,
    deser_key: PhantomData<fn() -> K>,
}

impl<K: AsRef<[u8]>, V> ReadonlyRocksDBDict<K, V> {
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
        match self.db.get_pinned(key.as_ref())? {
            None => Ok(None),
            Some(value) => (self.deser_value)(value.as_ref()).map(|v| Some(v)),
        }
    }

    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
    {
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
        deser_value: deser_entity,
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
        deser_value: deser_entity_metadata,
        deser_key: PhantomData,
    })
}

pub fn open_entity_link_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityOutLink>, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Lz4);

    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;
    Ok(ReadonlyRocksDBDict {
        db,
        deser_value: deser_entity_link,
        deser_key: PhantomData,
    })
}

pub fn open_entity_pagerank_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, f64>, KGDataError> {
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
        deser_value: deser_entity_pagerank,
        deser_key: PhantomData,
    })
}

pub fn open_property_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, Property>, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Lz4);

    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;
    Ok(ReadonlyRocksDBDict {
        db,
        deser_value: deser_property,
        deser_key: PhantomData,
    })
}

pub fn open_class_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Class>, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);

    let db = rocksdb::DB::open_for_read_only(&options, dbpath, false)?;
    Ok(ReadonlyRocksDBDict {
        db,
        deser_value: deser_class,
        deser_key: PhantomData,
    })
}

pub fn deser_entity(v: &[u8]) -> Result<Entity, KGDataError> {
    Ok(serde_json::from_slice::<WDEntity>(v)?.0)
}

fn deser_entity_metadata(v: &[u8]) -> Result<EntityMetadata, KGDataError> {
    Ok(serde_json::from_slice::<WDEntityMetadata>(v)?.0)
}

fn deser_entity_pagerank(v: &[u8]) -> Result<f64, KGDataError> {
    Ok(f64::from_le_bytes(v.try_into()?))
}

fn deser_entity_link(v: &[u8]) -> Result<EntityOutLink, KGDataError> {
    Ok(serde_json::from_slice::<EntityOutLink>(v)?)
}

fn deser_property(v: &[u8]) -> Result<Property, KGDataError> {
    Ok(serde_json::from_slice::<WDProperty>(v)?.0)
}

fn deser_class(v: &[u8]) -> Result<Class, KGDataError> {
    Ok(serde_json::from_slice::<WDClass>(v)?.0)
}
