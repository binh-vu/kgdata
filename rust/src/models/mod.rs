///! This module contains the models for entities, classes, and properties in a knowledge graph.
///!  
///! We adopt the Wikidata model and use it for other knowledge graphs as well.

/// Entity in Knowledge Graph.
pub mod entity;
pub mod entity_link;
pub mod entity_metadata;
pub mod multilingual;
pub mod value;

pub mod class;
pub mod property;

pub mod kgns;

pub use self::class::*;
pub use self::entity::*;
pub use self::entity_link::*;
pub use self::entity_metadata::*;
pub use self::multilingual::*;
pub use self::property::*;
pub use self::value::*;
