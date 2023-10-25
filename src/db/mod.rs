use std::borrow::Borrow;
use std::ffi::OsStr;
use std::path::PathBuf;

use crate::conversions::{WDClass, WDEntityMetadata, WDProperty};
use crate::models::kgns::KnowledgeGraphNamespace;
use crate::models::{Class, Entity, EntityMetadata, EntityOutLink, Property};
use crate::{conversions::WDEntity, error::KGDataError};
use serde_json;

mod interface;
mod predefined_db;
mod readonly_rocksdb_dict;
mod remotedb;
pub use self::interface::Dict;
pub use self::predefined_db::{
    open_big_db, open_medium_db, open_nocompressed_db, open_small_db, PredefinedDB,
};
pub use self::readonly_rocksdb_dict::ReadonlyRocksDBDict;
pub use self::remotedb::{dial, serve_db, RemoteRocksDBDict, RepMsg, ReqMsg};

pub struct BaseKGDB<ED, EMD>
where
    ED: Dict<String, Entity> + Sync + Send,
    EMD: Dict<String, EntityMetadata> + Sync + Send,
{
    pub datadir: PathBuf,
    pub classes: ReadonlyRocksDBDict<String, Class>,
    pub props: ReadonlyRocksDBDict<String, Property>,
    pub entities: ED,
    pub entity_redirection: ReadonlyRocksDBDict<String, String>,
    pub entity_metadata: EMD,
    pub entity_outlink: ReadonlyRocksDBDict<String, EntityOutLink>,
    pub entity_pagerank: ReadonlyRocksDBDict<String, f64>,
    pub kgns: KnowledgeGraphNamespace,
}

pub type KGDB =
    BaseKGDB<ReadonlyRocksDBDict<String, Entity>, ReadonlyRocksDBDict<String, EntityMetadata>>;
pub type RemoteKGDB =
    BaseKGDB<RemoteRocksDBDict<String, Entity>, RemoteRocksDBDict<String, EntityMetadata>>;

impl KGDB {
    pub fn new(datadir: &str) -> Result<Self, KGDataError> {
        let datadir = PathBuf::from(datadir);
        Ok(Self {
            props: open_property_db(datadir.join("props.db").as_os_str())?,
            classes: open_class_db(datadir.join("classes.db").as_os_str())?,
            entities: open_entity_db(datadir.join("entities.db").as_os_str())?,
            entity_redirection: open_entity_redirection_db(
                datadir.join("entity_redirections.db").as_os_str(),
            )?,
            entity_metadata: open_entity_metadata_db(
                datadir.join("entity_metadata.db").as_os_str(),
            )?,
            entity_outlink: open_entity_outlink_db(datadir.join("entity_outlinks.db").as_os_str())?,
            entity_pagerank: open_entity_pagerank_db(
                datadir.join("entity_pagerank.db").as_os_str(),
            )?,
            datadir,
            kgns: KnowledgeGraphNamespace::wikidata(),
        })
    }

    #[inline]
    pub fn open_entity_raw_db(datadir: PathBuf) -> Result<rocksdb::DB, KGDataError> {
        open_big_db(datadir.join("entities.db").as_os_str())
    }

    #[inline]
    pub fn open_entity_metadata_raw_db(datadir: PathBuf) -> Result<rocksdb::DB, KGDataError> {
        open_big_db(datadir.join("entity_metadata.db").as_os_str())
    }

    #[inline]
    pub fn open_entity_redirection_raw_db(datadir: PathBuf) -> Result<rocksdb::DB, KGDataError> {
        open_small_db(datadir.join("entity_redirections.db").as_os_str())
    }
}

pub fn open_entity_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Entity>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(open_big_db(dbpath)?, deser_entity))
}

pub fn open_entity_metadata_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityMetadata>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_big_db(dbpath)?,
        deser_entity_metadata,
    ))
}

pub fn open_entity_redirection_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, String>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_small_db(dbpath)?,
        deser_string,
    ))
}

pub fn open_entity_outlink_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, EntityOutLink>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_small_db(dbpath)?,
        deser_entity_outlink,
    ))
}

pub fn open_entity_pagerank_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, f64>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_nocompressed_db(dbpath)?,
        deser_entity_pagerank,
    ))
}

pub fn open_property_db(
    dbpath: &OsStr,
) -> Result<ReadonlyRocksDBDict<String, Property>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_small_db(dbpath)?,
        deser_property,
    ))
}

pub fn open_class_db(dbpath: &OsStr) -> Result<ReadonlyRocksDBDict<String, Class>, KGDataError> {
    Ok(ReadonlyRocksDBDict::new(
        open_medium_db(dbpath)?,
        deser_class,
    ))
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
