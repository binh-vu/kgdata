use crate::models::multilingual::{MultiLingualString, MultiLingualStringList};

use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::pyview;
use pyo3::prelude::*;
use pyo3::types::PyList;

pyview!(
    MultiLingualStringView(module = "kgdata.core.models", name = "MultiLingualStringView", cls = MultiLingualString) {
        b(lang: String),
    }
);

#[pymethods]
impl MultiLingualStringView {
    pub fn as_lang_default(&self) -> &str {
        &self.0.lang2value[&self.0.lang]
    }

    pub fn as_lang(&self, lang: &str) -> PyResult<&str> {
        if !self.0.lang2value.contains_key(lang) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "No language {}",
                lang
            )));
        }
        Ok(&self.0.lang2value[lang])
    }
}

pyview!(
    MultiLingualStringListView(module = "kgdata.core.models", name = "MultiLingualStringListView", cls = MultiLingualStringList) {
        b(lang: String),
    }
);

#[pymethods]
impl MultiLingualStringListView {
    pub fn as_lang_default<'t>(&'t self, py: Python<'t>) -> &'t PyList {
        PyList::new(py, &self.0.lang2values[&self.0.lang])
    }

    pub fn as_lang<'t>(&'t self, py: Python<'t>, lang: &str) -> PyResult<&'t PyList> {
        if !self.0.lang2values.contains_key(lang) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "No language {}",
                lang
            )));
        }
        Ok(PyList::new(py, &self.0.lang2values[lang]))
    }
}
