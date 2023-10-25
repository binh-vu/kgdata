use std::{ffi::OsStr, path::PathBuf};

use rocksdb::{DBCompressionType, Options};

use crate::error::KGDataError;

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
            PredefinedDB::Entity => "entity.db",
            PredefinedDB::EntityMetadata => "entity_metadata.db",
            PredefinedDB::EntityOutLink => "entity_outlinks.db",
            PredefinedDB::EntityRedirection => "entity_redirections.db",
            PredefinedDB::EntityPageRank => "entity_pagerank.db",
            PredefinedDB::Property => "props.db",
            PredefinedDB::Class => "classes.db",
        }
    }

    pub fn open_raw_db(&self, datadir: &str) -> Result<rocksdb::DB, KGDataError> {
        let tmp = PathBuf::from(datadir).join(self.get_dbname());
        let dbpath = tmp.as_os_str();
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
