use std::ffi::OsStr;

use rocksdb::{DBCompressionType, Options};

use crate::error::KGDataError;

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
        let dbpath = PathBuf::from(datadir).join(self.get_dbname()).as_os_str();
        match self {
            Predefined::Entity => open_big_db(dbpath),
            Predefined::EntityMetadata => open_big_db(dbpath),
            Predefined::EntityOutLink => open_small_db(dbpath),
            Predefined::EntityRedirection => open_small_db(dbpath),
            Predefined::EntityPageRank => open_nocompressed_db(dbpath),
            Predefined::Property => open_small_db(dbpath),
            Predefined::Class => open_medium_db(dbpath),
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
