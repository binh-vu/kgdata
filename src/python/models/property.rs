use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use crate::models::{DataType, Property};
use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::pyo3helper::{list_str_view, map_usize_view};
use crate::{pyview, pywrap};
use pyo3::prelude::*;

pyview!(
    PropertyView(module = "kgdata.core.models", name = "PropertyView", cls = Property) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        b(datatype: DataType),
        v(parents: list_str_view::ListView),
        v(related_properties: list_str_view::ListView),
        v(equivalent_properties: list_str_view::ListView),
        v(domains: list_str_view::ListView),
        v(ranges: list_str_view::ListView),
        v(inverse_properties: list_str_view::ListView),
        v(instanceof: list_str_view::ListView),
        v(ancestors: map_usize_view::MapView),
        f(is_object_property: bool),
        f(is_data_property: bool),
    }
);

pywrap!(
    PyProperty(module = "kgdata.core.models", name = "Property", cls = Property) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        b(datatype: DataType),
        v(parents: list_str_view::ListView),
        v(related_properties: list_str_view::ListView),
        v(equivalent_properties: list_str_view::ListView),
        v(domains: list_str_view::ListView),
        v(ranges: list_str_view::ListView),
        v(inverse_properties: list_str_view::ListView),
        v(instanceof: list_str_view::ListView),
        v(ancestors: map_usize_view::MapView),
        f(is_object_property: bool),
        f(is_data_property: bool),
    }
);
