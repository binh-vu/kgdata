use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use crate::models::EntityMetadata;
use crate::pyo3helper::list_str_view;
use crate::pywrap;
use hashbrown::HashSet;
use pyo3::prelude::*;

pywrap!(
    PyEntityMetadata(module = "kgdata.core.models", name = "EntityMetadata", cls = EntityMetadata) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(instanceof: list_str_view::ListView),
        v(subclassof: list_str_view::ListView),
        v(subpropertyof: list_str_view::ListView),
    }
);

#[pymethods]
impl PyEntityMetadata {
    /// Get all aliases of an entity (all names except the default label)
    pub fn get_all_aliases(&self) -> Vec<&String> {
        let mut aliases = HashSet::with_capacity(self.0.label.lang2value.len());
        aliases.extend(self.0.label.lang2value.values());
        aliases.extend(self.0.aliases.lang2values.values().flatten());
        aliases.remove(self.0.label.get_default_value());
        aliases.into_iter().collect::<Vec<_>>()
    }
}
