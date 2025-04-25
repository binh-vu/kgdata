use hashbrown::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityOutLink {
    pub id: String,               // source entity id
    pub targets: HashSet<String>, // target entity ids
}
