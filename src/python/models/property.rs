use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use crate::models::{DataType, Property};
use crate::pyo3helper::{list_str_view, set_str_view};
use crate::pywrap;
use pyo3::prelude::*;

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
        v(subjects: list_str_view::ListView),
        v(inverse_properties: list_str_view::ListView),
        v(instanceof: list_str_view::ListView),
        v(ancestors: set_str_view::SetView),
        f(is_object_property: bool),
        f(is_data_property: bool),
    }
);

impl IntoPy<PyObject> for &DataType {
    fn into_py(self, py: Python) -> PyObject {
        self.to_str().into_py(py)
    }
}
