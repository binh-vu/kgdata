use super::Property;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum KGName {
    Wikidata,
    DBpedia,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KnowledgeGraphNamespace {
    kgname: KGName,
    pub instanceof: String,
}

impl KnowledgeGraphNamespace {
    pub fn wikidata() -> Self {
        Self {
            kgname: KGName::Wikidata,
            instanceof: "P31".to_owned(),
        }
    }

    pub fn is_transitive_property(&self, prop: &Property) -> bool {
        match self.kgname {
            KGName::Wikidata => prop.instanceof.iter().any(|x| x == "Q18647515"),
            KGName::DBpedia => false,
        }
    }
}
