use crate::models::multilingual::{MultiLingualString, MultiLingualStringList};

use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use pyo3::prelude::*;
use pyo3::types::PyList;

// pyview!(
//     MultiLingualStringView(module = "kgdata.core.models", name = "MultiLingualStringView", cls = MultiLingualString) {
//         b(lang: String),
//         b(lang2value: std::collections::HashMap<String, String>),
//     }
// )

#[pyclass(module = "kgdata.core.models", name = "MultiLingualStringView")]
pub struct MultiLingualStringView(pub &'static MultiLingualString);

impl MultiLingualStringView {
    pub fn new(value: &MultiLingualString) -> Self {
        Self(unsafe_update_view_lifetime_signature(value))
    }
}

#[pymethods]
impl MultiLingualStringView {
    #[getter]
    pub fn default_lang(&self) -> &str {
        &self.0.lang
    }

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

    pub fn to_list(&self) -> Vec<&String> {
        self.0.lang2value.values().collect::<Vec<_>>()
    }
}

#[pyclass(module = "kgdata.core.models", name = "MultiLingualStringListView")]
pub struct MultiLingualStringListView(pub &'static MultiLingualStringList);

impl MultiLingualStringListView {
    pub fn new(value: &MultiLingualStringList) -> Self {
        Self(unsafe_update_view_lifetime_signature(value))
    }
}

#[pymethods]
impl MultiLingualStringListView {
    #[getter]
    pub fn default_lang(&self) -> &str {
        &self.0.lang
    }

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

    pub fn flatten(&self) -> Vec<&String> {
        self.0.lang2values.values().flatten().collect::<Vec<_>>()
    }
}
