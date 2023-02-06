use super::entity::EntityType;
use serde::{Deserialize, Serialize};

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
    #[serde(alias = "upperBound")]
    pub upper_bound: Option<String>,
    // Optionally, the lower bound of the quantity's uncertainty interval, using the same notation as the amount field. If not given or null, the uncertainty (or precision) of the quantity is not known. If the lowerBound field is given, the upperBound field must also be given.
    #[serde(alias = "lowerBound")]
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
