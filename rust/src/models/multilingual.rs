use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiLingualString {
    pub lang: String,
    pub lang2value: HashMap<String, String>,
}

impl MultiLingualString {
    pub fn get_default_value(&self) -> &String {
        self.lang2value.get(&self.lang).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MultiLingualStringList {
    pub lang: String,
    pub lang2values: HashMap<String, Vec<String>>,
}

impl MultiLingualStringList {
    pub fn get_default_values(&self) -> &Vec<String> {
        self.lang2values.get(&self.lang).unwrap()
    }
}
