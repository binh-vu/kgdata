use hashbrown::HashSet;
use pyo3::{
    prelude::*,
    types::{PyList, PySet},
};

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
