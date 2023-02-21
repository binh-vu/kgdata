use super::super::entity::Entity;
use super::super::multilingual::MultiLingualString;

use pyo3::prelude::*;
use pyo3::types::PyString;
use std::rc::Rc;

#[pyclass(module = "kgdata.core.models", unsendable, name = "MultiLingualString")]
pub struct PyEntMultiLingualString {
    _container: Rc<Entity>,
    value: Py<PyString>,
    object: *const MultiLingualString,
}

#[pyclass]
pub struct PyEntLingualString {}

impl PyEntMultiLingualString {
    pub fn new(py: Python<'_>, container: Rc<Entity>, object: &MultiLingualString) -> Self {
        PyEntMultiLingualString {
            _container: container,
            object: object as *const MultiLingualString,
            value: PyString::new(py, &object.lang2value[&object.lang]).into(),
        }
    }
}

#[pymethods]
impl PyEntMultiLingualString {
    fn to_str(&self) -> &Py<PyString> {
        &self.value
    }

    fn to_lang<'s>(&self, py: Python<'s>, lang: &str) -> PyResult<&'s PyString> {
        let obj = unsafe { &*self.object };
        let it = obj.lang2value.keys();
        if !obj.lang2value.contains_key(lang) {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(format!(
                "No language {}",
                lang
            )));
        }
        Ok(PyString::new(py, &obj.lang2value[lang]))
    }
}
