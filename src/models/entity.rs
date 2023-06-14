use crate::error::KGDataError;

use super::multilingual::{MultiLingualString, MultiLingualStringList};
use super::value::Value;
use hashbrown::HashMap;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass(module = "kgdata.core.models", name = "EntityType")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EntityType {
    #[serde(rename = "item")]
    Item,
    #[serde(rename = "property")]
    Property,
}

impl EntityType {
    pub fn to_str(&self) -> &'static str {
        match self {
            EntityType::Item => "item",
            EntityType::Property => "property",
        }
    }

    pub fn from_str(s: &str) -> Result<EntityType, KGDataError> {
        match s {
            "item" => Ok(EntityType::Item),
            "property" => Ok(EntityType::Property),
            _ => Err(KGDataError::ValueError(format!(
                "Unknown entity type: {}",
                s
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entity {
    pub id: String,
    pub entity_type: EntityType,
    pub label: MultiLingualString,
    pub description: MultiLingualString,
    pub aliases: MultiLingualStringList,
    pub sitelinks: HashMap<String, SiteLink>,
    pub props: HashMap<String, Vec<Statement>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SiteLink {
    pub site: String,
    pub title: String,
    pub badges: Vec<String>,
    pub url: Option<String>,
}

#[pyclass(module = "kgdata.core.models", name = "StatementRank")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum StatementRank {
    #[serde(rename = "normal")]
    Normal,
    #[serde(rename = "preferred")]
    Preferred,
    #[serde(rename = "deprecated")]
    Deprecated,
}

impl StatementRank {
    pub fn to_str(&self) -> &'static str {
        match self {
            StatementRank::Normal => "normal",
            StatementRank::Preferred => "preferred",
            StatementRank::Deprecated => "deprecated",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Statement {
    pub value: Value,
    pub qualifiers: HashMap<String, Vec<Value>>,
    pub qualifiers_order: Vec<String>,
    pub rank: StatementRank,
}
