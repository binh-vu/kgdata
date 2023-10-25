use std::borrow::Borrow;
use std::ffi::OsStr;
use std::path::PathBuf;

use crate::conversions::{WDClass, WDEntityMetadata, WDProperty};
use crate::models::kgns::KnowledgeGraphNamespace;
use crate::models::{Class, Entity, EntityMetadata, EntityOutLink, Property};
use crate::{conversions::WDEntity, error::KGDataError};
use serde_json;


/// A persistent key-value store
pub trait PersistentDict<K: AsRef<[u8]>, V>: Send + Sync {
    // /// Get multiple keys
    // fn batch_get<Q: ?Sized>(&self, keys: I) -> Result<Vec<Option<V>, KGDataError>
    // where
    //     K: Borrow<Q>,
    //     Q: AsRef<[u8]>,
    //     I: Iterator<Item = &Q>;

    /// Get a key
    fn get<Q: ?Sized>(&self, key: &Q) -> Result<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>;

    /// Check if a key exists
    fn contains_key<Q: ?Sized>(&self, key: &Q) -> Result<bool, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>;
}

pub trait ParallelDict<K: AsRef<[u8]>, V>: Send + Sync {
    /// Get multiple keys
    fn batch_get<Q: ?Sized>(&self, keys: I) -> Result<Vec<Option<V>, KGDataError>
    where
        K: Borrow<Q>,
        Q: AsRef<[u8]>,
        I: IntoIterator<Item = &Q>;

    fn par_batch_get<Q: ?Sized>(&self, keys: I) -> Result<Vec<Option<V>, KGDataError>
        where
            K: Borrow<Q>,
            Q: AsRef<[u8]>,
            I: IntoIterator<Item = &Q>;
}