///! This module contains the models for entities, classes, and properties in a knowledge graph.
///!  
///! We adopt the Wikidata model and use it for other knowledge graphs as well.

/// Entity in Knowledge Graph.
pub mod entity;
pub mod multilingual;
pub mod python;
pub mod value;

pub use self::entity::*;
pub use self::multilingual::*;
pub use self::value::*;