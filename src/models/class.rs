use super::{MultiLingualString, MultiLingualStringList};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Class {
    pub id: String,
    pub label: MultiLingualString,
    pub description: MultiLingualString,
    pub aliases: MultiLingualStringList,
    pub parents: Vec<String>,
    pub properties: Vec<String>,
    pub different_froms: Vec<String>, // properties of a type
    pub equivalent_classes: Vec<String>,
    pub ancestors: HashMap<String, usize>,
}
