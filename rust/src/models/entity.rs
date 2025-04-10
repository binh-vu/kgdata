use super::multilingual::{MultiLingualString, MultiLingualStringList};
use super::value::Value;
use hashbrown::HashMap;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entity {
    pub id: String,
    pub label: MultiLingualString,
    pub description: MultiLingualString,
    pub aliases: MultiLingualStringList,
    pub props: HashMap<String, Vec<Statement>>,
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

    pub fn intersect(&self, another: &StatementRank) -> StatementRank {
        match self {
            StatementRank::Normal => match another {
                StatementRank::Normal => StatementRank::Normal,
                StatementRank::Preferred => StatementRank::Normal,
                StatementRank::Deprecated => StatementRank::Deprecated,
            },
            StatementRank::Preferred => match another {
                StatementRank::Normal => StatementRank::Normal,
                StatementRank::Preferred => StatementRank::Preferred,
                StatementRank::Deprecated => StatementRank::Deprecated,
            },
            StatementRank::Deprecated => StatementRank::Deprecated,
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
