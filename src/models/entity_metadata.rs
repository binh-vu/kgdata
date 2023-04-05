use super::multilingual::{MultiLingualString, MultiLingualStringList};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EntityMetadata {
    pub id: String,
    pub label: MultiLingualString,
    pub description: MultiLingualString,
    pub aliases: MultiLingualStringList,
    pub instanceof: Vec<String>,
    pub subclassof: Vec<String>,
    pub subpropertyof: Vec<String>,
}
