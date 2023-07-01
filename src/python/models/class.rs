use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use crate::models::{Class, MultiLingualString};
use crate::pyo3helper::{list_str_view, set_str_view, PySetString};
use crate::pywrap;
use hashbrown::HashSet;
use pyo3::prelude::*;

pywrap!(
    PyClass(module = "kgdata.core.models", name = "Class", cls = Class) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(parents: list_str_view::ListView),
        v(properties: list_str_view::ListView),
        v(different_froms: list_str_view::ListView),
        v(equivalent_classes: list_str_view::ListView),
        v(ancestors: set_str_view::SetView),
    }
);

#[pymethods]
impl PyClass {
    #[new]
    pub fn init(
        id: String,
        // label: MultiLingualString,
        // description: MultiLingualString,
        // aliases: MultiLingualStringList,
        // parents: Vec<String>,
        // properties: Vec<String>,
        // different_froms: Vec<String>,
        // equivalent_classes: Vec<String>,
        ancestors: PySetString,
    ) -> Self {
        unimplemented!()
        // Self {
        //     id,
        //     label,
        //     description,
        //     aliases,
        //     parents,
        //     properties,
        //     different_froms,
        //     equivalent_classes,
        //     ancestors,
        // }
    }
}
