pub struct KnowledgeGraphNamespace {
    pub instanceof: String,
}

impl KnowledgeGraphNamespace {
    pub fn wikidata() -> Self {
        Self {
            instanceof: "P31".to_owned(),
        }
    }
}
