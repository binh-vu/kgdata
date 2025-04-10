use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::error::KGDataError;

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
pub struct EntityId {
    pub id: String,
    #[serde(rename = "entity-type")]
    pub entity_type: EntityType,
    #[serde(rename = "numeric-id")]
    pub numeric_id: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Time {
    // See more: https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html
    pub time: String,
    pub timezone: u64,
    pub before: u64,
    pub after: u64,
    pub precision: u64,
    pub calendarmodel: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Quantity {
    // The nominal value of the quantity, as an arbitrary precision decimal string. The string always starts with a character indicating the sign of the value, either “+” or “-”.
    pub amount: String,
    // Optionally, the upper bound of the quantity's uncertainty interval, using the same notation as the amount field. If not given or null, the uncertainty (or precision) of the quantity is not known. If the upperBound field is given, the lowerBound field must also be given.
    #[serde(rename = "upperBound")]
    pub upper_bound: Option<String>,
    // Optionally, the lower bound of the quantity's uncertainty interval, using the same notation as the amount field. If not given or null, the uncertainty (or precision) of the quantity is not known. If the lowerBound field is given, the upperBound field must also be given.
    #[serde(rename = "lowerBound")]
    pub lower_bound: Option<String>,
    // The URI of a unit (or “1” to indicate a unit-less quantity). This would typically refer to a data item on wikidata.org, e.g. http://www.wikidata.org/entity/Q712226 for “square kilometer”.
    pub unit: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GlobeCoordinate {
    pub latitude: f64,
    pub longitude: f64,
    pub precision: Option<f64>,
    pub altitude: Option<f64>,
    // The URI of a reference globe. This would typically refer to a data item on wikidata.org. This is usually just an indication of the celestial body (e.g. Q2 = earth), but could be more specific, like WGS 84 or ED50.
    pub globe: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MonolingualText {
    pub text: String,
    pub language: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Value {
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "entity-id")]
    EntityId(EntityId),
    #[serde(rename = "time")]
    Time(Time),
    #[serde(rename = "quantity")]
    Quantity(Quantity),
    #[serde(rename = "monolingualtext")]
    MonolingualText(MonolingualText),
    #[serde(rename = "globecoordinate")]
    GlobeCoordinate(GlobeCoordinate),
}

impl Value {
    pub fn get_type(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::EntityId(_) => "entity-id",
            Value::Quantity(_) => "quantity",
            Value::Time(_) => "time",
            Value::GlobeCoordinate(_) => "globe-coordinate",
            Value::MonolingualText(_) => "monolingual-text",
        }
    }

    pub fn is_str(&self) -> bool {
        match self {
            Value::String(_) => true,
            _ => false,
        }
    }

    pub fn is_entity_id(&self) -> bool {
        match self {
            Value::EntityId(_) => true,
            _ => false,
        }
    }

    pub fn is_quantity(&self) -> bool {
        match self {
            Value::Quantity(_) => true,
            _ => false,
        }
    }

    pub fn is_time(&self) -> bool {
        match self {
            Value::Time(_) => true,
            _ => false,
        }
    }

    pub fn is_globe_coordinate(&self) -> bool {
        match self {
            Value::GlobeCoordinate(_) => true,
            _ => false,
        }
    }

    pub fn is_monolingual_text(&self) -> bool {
        match self {
            Value::MonolingualText(_) => true,
            _ => false,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_entity_id(&self) -> Option<&EntityId> {
        match self {
            Value::EntityId(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<&Time> {
        match self {
            Value::Time(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_quantity(&self) -> Option<&Quantity> {
        match self {
            Value::Quantity(q) => Some(q),
            _ => None,
        }
    }

    pub fn as_monolingual_text(&self) -> Option<&MonolingualText> {
        match self {
            Value::MonolingualText(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_globe_coordinate(&self) -> Option<&GlobeCoordinate> {
        match self {
            Value::GlobeCoordinate(g) => Some(g),
            _ => None,
        }
    }

    pub fn to_string_repr(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}
