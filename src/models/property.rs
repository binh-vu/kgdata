use crate::error::KGDataError;

use super::{MultiLingualString, MultiLingualStringList};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Property {
    pub id: String,
    pub label: MultiLingualString,
    pub description: MultiLingualString,
    pub aliases: MultiLingualStringList,
    pub datatype: DataType,
    pub instanceof: Vec<String>,
    pub parents: Vec<String>,
    pub ancestors: HashMap<String, usize>,
    pub inverse_properties: Vec<String>,
    pub related_properties: Vec<String>,
    pub equivalent_properties: Vec<String>,
    pub domains: Vec<String>,
    pub ranges: Vec<String>,
}

impl Property {
    pub fn is_object_property(&self) -> bool {
        self.datatype == DataType::WikibaseItem
    }

    pub fn is_data_property(&self) -> bool {
        !self.is_object_property()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum DataType {
    WikibaseLexeme,
    Monolingualtext,
    WikibaseSense,
    Url,
    WikibaseProperty,
    WikibaseForm,
    ExternalId,
    Time,
    #[serde(rename = "commonsMedia")]
    CommonsMedia,
    Quantity,
    WikibaseItem,
    MusicalNotation,
    TabularData,
    String,
    Math,
    GeoShape,
    GlobeCoordinate,
}

impl DataType {
    pub fn to_str(&self) -> &'static str {
        match self {
            DataType::WikibaseLexeme => "wikibase-lexeme",
            DataType::Monolingualtext => "monolingualtext",
            DataType::WikibaseSense => "wikibase-sense",
            DataType::Url => "url",
            DataType::WikibaseProperty => "wikibase-property",
            DataType::WikibaseForm => "wikibase-form",
            DataType::ExternalId => "external-id",
            DataType::Time => "time",
            DataType::CommonsMedia => "commonsMedia",
            DataType::Quantity => "quantity",
            DataType::WikibaseItem => "wikibase-item",
            DataType::MusicalNotation => "musical-notation",
            DataType::TabularData => "tabular-data",
            DataType::String => "string",
            DataType::Math => "math",
            DataType::GeoShape => "geo-shape",
            DataType::GlobeCoordinate => "globe-coordinate",
        }
    }

    pub fn from_str(s: &str) -> Result<DataType, KGDataError> {
        match s {
            "wikibase-lexeme" => Ok(DataType::WikibaseLexeme),
            "monolingualtext" => Ok(DataType::Monolingualtext),
            "wikibase-sense" => Ok(DataType::WikibaseSense),
            "url" => Ok(DataType::Url),
            "wikibase-property" => Ok(DataType::WikibaseProperty),
            "wikibase-form" => Ok(DataType::WikibaseForm),
            "external-id" => Ok(DataType::ExternalId),
            "time" => Ok(DataType::Time),
            "commonsMedia" => Ok(DataType::CommonsMedia),
            "quantity" => Ok(DataType::Quantity),
            "wikibase-item" => Ok(DataType::WikibaseItem),
            "musical-notation" => Ok(DataType::MusicalNotation),
            "tabular-data" => Ok(DataType::TabularData),
            "string" => Ok(DataType::String),
            "math" => Ok(DataType::Math),
            "geo-shape" => Ok(DataType::GeoShape),
            "globe-coordinate" => Ok(DataType::GlobeCoordinate),
            _ => Err(KGDataError::ValueError(format!("Unknown data type: {}", s))),
        }
    }
}
