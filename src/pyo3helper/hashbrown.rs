use hashbrown::HashSet;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PySet},
};
use serde::{Deserialize, Serialize};

/// An zero-cost abstraction for automatically receiving HashSet<String> from Python.
pub struct PySetString(pub HashSet<String>);

impl<'t> FromPyObject<'t> for PySetString {
    fn extract(obj: &'t PyAny) -> PyResult<Self> {
        if let Ok(lst) = obj.downcast::<PyList>() {
            return Ok(PySetString(
                lst.into_iter()
                    .map(|x| x.extract())
                    .collect::<PyResult<HashSet<String>>>()?,
            ));
        }
        if let Ok(obj) = obj.downcast::<PySet>() {
            return Ok(PySetString(
                obj.into_iter()
                    .map(|x| x.extract())
                    .collect::<PyResult<HashSet<String>>>()?,
            ));
        }
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected a list or set of strings",
        ));
    }
}

#[pyclass]
// #[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Map(pub hashbrown::HashMap<String, String>);

impl Map {
    pub fn new(map: hashbrown::HashMap<String, String>) -> Self {
        Self(map)
    }
}

#[pymethods]
impl Map {
    #[new]
    pub fn init(py: Python<'_>, obj: PyObject) -> PyResult<Self> {
        obj.extract(py)
        // if let Ok(map) = obj.downcast::<PyDict>() {
        //     return Ok(Map(map
        //         .into_iter()
        //         .map(|x| {
        //             let (k, v) = x?;
        //             Ok((k.extract::<String>()?, v.extract::<String>()?))
        //         })
        //         .collect::<PyResult<_>>()?));
        // }
        // if let Ok(lst) = obj.downcast::<PyList>() {

        // }
    }

    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __getitem__(&self, k: &str) -> PyResult<&str> {
        if let Some(v) = self.0.get(k) {
            Ok(v)
        } else {
            Err(pyo3::exceptions::PyKeyError::new_err(format!(
                "Key not found: {}",
                k
            )))
        }
    }
}

impl<'t> FromPyObject<'t> for Map {
    fn extract(obj: &'t PyAny) -> PyResult<Self> {
        if let Ok(obj) = obj.downcast::<PyDict>() {
            return Ok(Map(obj
                .into_iter()
                .map(|(k, v)| Ok((k.extract()?, v.extract()?)))
                .collect::<PyResult<_>>()?));
        }
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected a dictionary",
        ));
    }
}
