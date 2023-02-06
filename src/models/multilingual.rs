use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiLingualString {
    pub lang: String,
    pub lang2value: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiLingualStringList {
    pub lang: String,
    pub lang2values: HashMap<String, Vec<String>>,
}
