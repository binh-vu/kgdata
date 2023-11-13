use std::path::PathBuf;

use crate::error::KGDataError;
use crate::models::kgns::KnowledgeGraphNamespace;
use crate::models::{Class, Entity, EntityMetadata, EntityOutLink, Property};

mod interface;
mod predefined_db;
mod readonly_rocksdb_dict;
pub mod remotedb;
pub use self::interface::Map;
pub use self::predefined_db::{
    deser_entity, deser_entity_metadata, open_class_db, open_entity_db, open_entity_metadata_db,
    open_entity_outlink_db, open_entity_pagerank_db, open_entity_redirection_db, open_property_db,
    PredefinedDB,
};
pub use self::readonly_rocksdb_dict::ReadonlyRocksDBDict;
pub use self::remotedb::{serve_db, RemoteRocksDBDict};

pub struct BaseKGDB<ED, EMD>
where
    ED: Map<String, Entity> + Sync + Send,
    EMD: Map<String, EntityMetadata> + Sync + Send,
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
            props: open_property_db(
                datadir
                    .join(PredefinedDB::Property.get_dbname())
                    .as_os_str(),
            )?,
            classes: open_class_db(datadir.join(PredefinedDB::Class.get_dbname()).as_os_str())?,
            entities: open_entity_db(datadir.join(PredefinedDB::Entity.get_dbname()).as_os_str())?,
            entity_redirection: open_entity_redirection_db(
                datadir
                    .join(PredefinedDB::EntityRedirection.get_dbname())
                    .as_os_str(),
            )?,
            entity_metadata: open_entity_metadata_db(
                datadir
                    .join(PredefinedDB::EntityMetadata.get_dbname())
                    .as_os_str(),
            )?,
            entity_outlink: open_entity_outlink_db(
                datadir
                    .join(PredefinedDB::EntityOutLink.get_dbname())
                    .as_os_str(),
            )?,
            entity_pagerank: open_entity_pagerank_db(
                datadir
                    .join(PredefinedDB::EntityPageRank.get_dbname())
                    .as_os_str(),
            )?,
            datadir,
            kgns: KnowledgeGraphNamespace::wikidata(),
        })
    }
}

impl RemoteKGDB {
    pub fn new<Q>(
        datadir: &str,
        entity_urls: &[Q],
        entity_metadata_urls: &[Q],
        entity_batch_size: usize,
        entity_metadata_batch_size: usize,
    ) -> Result<Self, KGDataError>
    where
        Q: AsRef<str>,
    {
        let datadir = PathBuf::from(datadir);
        Ok(Self {
            props: open_property_db(
                datadir
                    .join(PredefinedDB::Property.get_dbname())
                    .as_os_str(),
            )?,
            classes: open_class_db(datadir.join(PredefinedDB::Class.get_dbname()).as_os_str())?,
            entities: RemoteRocksDBDict::new(entity_urls, entity_batch_size, deser_entity)?,
            entity_redirection: open_entity_redirection_db(
                datadir
                    .join(PredefinedDB::EntityRedirection.get_dbname())
                    .as_os_str(),
            )?,
            entity_metadata: RemoteRocksDBDict::new(
                entity_metadata_urls,
                entity_metadata_batch_size,
                deser_entity_metadata,
            )?,
            entity_outlink: open_entity_outlink_db(
                datadir
                    .join(PredefinedDB::EntityOutLink.get_dbname())
                    .as_os_str(),
            )?,
            entity_pagerank: open_entity_pagerank_db(
                datadir
                    .join(PredefinedDB::EntityPageRank.get_dbname())
                    .as_os_str(),
            )?,
            datadir,
            kgns: KnowledgeGraphNamespace::wikidata(),
        })
    }
}
