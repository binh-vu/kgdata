use std::ffi::OsStr;

use rocksdb::{DBCompressionType, Options};

use crate::error::KGDataError;

use super::ReadonlyRocksDBDict;
use crate::conversions::WDEntity;
use crate::conversions::{WDClass, WDEntityMetadata, WDProperty};
use crate::models::{Class, Entity, EntityMetadata, EntityOutLink, Property};

#[derive(Debug, Clone)]
pub enum PredefinedDB {
    Entity,
    EntityMetadata,
    EntityOutLink,
    EntityRedirection,
    EntityPageRank,
    Property,
    Class,
}

impl PredefinedDB {
    pub fn get_dbname(&self) -> &'static str {
        match self {
            PredefinedDB::Entity => "entities.db",
            PredefinedDB::EntityMetadata => "entity_metadata.db",
            PredefinedDB::EntityOutLink => "entity_outlinks.db",
            PredefinedDB::EntityRedirection => "entity_redirections.db",
            PredefinedDB::EntityPageRank => "entity_pagerank.db",
            PredefinedDB::Property => "props.db",
            PredefinedDB::Class => "classes.db",
        }
    }

    pub fn open_raw_db(&self, dbpath: &OsStr) -> Result<rocksdb::DB, KGDataError> {
        match self {
            PredefinedDB::Entity => open_big_db(dbpath),
            PredefinedDB::EntityMetadata => open_big_db(dbpath),
            PredefinedDB::EntityOutLink => open_small_db(dbpath),
            PredefinedDB::EntityRedirection => open_small_db(dbpath),
            PredefinedDB::EntityPageRank => open_nocompressed_db(dbpath),
            PredefinedDB::Property => open_small_db(dbpath),
            PredefinedDB::Class => open_medium_db(dbpath),
        }
    }
}

impl<'s> From<&'s str> for PredefinedDB {
    fn from(s: &'s str) -> Self {
        match s {
            "entity" => PredefinedDB::Entity,
            "entity_metadata" => PredefinedDB::EntityMetadata,
            "entity_outlinks" => PredefinedDB::EntityOutLink,
            "entity_redirections" => PredefinedDB::EntityRedirection,
            "entity_pagerank" => PredefinedDB::EntityPageRank,
            "props" => PredefinedDB::Property,
            "classes" => PredefinedDB::Class,
            _ => panic!("Unknown DB name: {}", s),
        }
    }
}

pub fn open_big_db(dbpath: &OsStr) -> Result<rocksdb::DB, KGDataError> {
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

    rocksdb::DB::open_for_read_only(&options, dbpath, false)
        .map_err(|e| KGDataError::RocksDBError(e))
}

pub fn open_medium_db(dbpath: &OsStr) -> Result<rocksdb::DB, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);

    rocksdb::DB::open_for_read_only(&options, dbpath, false)
        .map_err(|e| KGDataError::RocksDBError(e))
}

pub fn open_small_db(dbpath: &OsStr) -> Result<rocksdb::DB, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::Lz4);

    rocksdb::DB::open_for_read_only(&options, dbpath, false)
        .map_err(|e| KGDataError::RocksDBError(e))
}

pub fn open_nocompressed_db(dbpath: &OsStr) -> Result<rocksdb::DB, KGDataError> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.set_compression_type(DBCompressionType::None);

    rocksdb::DB::open_for_read_only(&options, dbpath, false)
        .map_err(|e| KGDataError::RocksDBError(e))
}

pub fn open_entity_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Entity>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::Entity.open_raw_db(dbpath)?,
        deser_entity,
    ))
}

pub fn open_entity_metadata_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityMetadata>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::EntityMetadata.open_raw_db(dbpath)?,
        deser_entity_metadata,
    ))
}

pub fn open_entity_redirection_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, String>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::EntityRedirection.open_raw_db(dbpath)?,
        deser_string,
    ))
}

pub fn open_entity_outlink_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityOutLink>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::EntityOutLink.open_raw_db(dbpath)?,
        deser_entity_outlink,
    ))
}

pub fn open_entity_pagerank_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, f64>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::EntityPageRank.open_raw_db(dbpath)?,
        deser_entity_pagerank,
    ))
}

pub fn open_property_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, Property>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::Property.open_raw_db(dbpath)?,
        deser_property,
    ))
}

pub fn open_class_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Class>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        PredefinedDB::Class.open_raw_db(dbpath)?,
        deser_class,
    ))
}

pub fn deser_entity(v: &[u8]) -> Result<Entity, KGDataError> {
    Ok(serde_json::from_slice::<WDEntity>(v)?.0)
}

pub fn deser_entity_metadata(v: &[u8]) -> Result<EntityMetadata, KGDataError> {
    Ok(serde_json::from_slice::<WDEntityMetadata>(v)?.0)
}

fn deser_entity_pagerank(v: &[u8]) -> Result<f64, KGDataError> {
    Ok(f64::from_le_bytes(v.try_into()?))
}

fn deser_entity_outlink(v: &[u8]) -> Result<EntityOutLink, KGDataError> {
    Ok(serde_json::from_slice::<EntityOutLink>(v)?)
}

fn deser_property(v: &[u8]) -> Result<Property, KGDataError> {
    Ok(serde_json::from_slice::<WDProperty>(v)?.0)
}

fn deser_class(v: &[u8]) -> Result<Class, KGDataError> {
    Ok(serde_json::from_slice::<WDClass>(v)?.0)
}

fn deser_string(v: &[u8]) -> Result<String, KGDataError> {
    String::from_utf8(v.to_owned()).map_err(KGDataError::from)
}
