use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use crate::models::Class;
use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::pyo3helper::{list_str_view, map_usize_view};
use crate::{pyview, pywrap};
use pyo3::prelude::*;

pyview!(
    ClassView(module = "kgdata.core.models", name = "ClassView", cls = Class) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(parents: list_str_view::ListView),
        v(properties: list_str_view::ListView),
        v(different_froms: list_str_view::ListView),
        v(equivalent_classes: list_str_view::ListView),
        v(ancestors: map_usize_view::MapView),
    }
);

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
        v(ancestors: map_usize_view::MapView),
    }
);
