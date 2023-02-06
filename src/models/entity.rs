use super::multilingual::{MultiLingualString, MultiLingualStringList};
use super::value::Value;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
