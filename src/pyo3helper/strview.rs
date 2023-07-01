use pyo3::prelude::*;

use super::unsafe_update_view_lifetime_signature;

/// An zero-cost abstraction for &str for returning &str to Python and accepting &str as the input because
/// receiving &String as input is not possible.
pub struct PyStrView(pub &'static str);

impl PyStrView {
    pub fn new(s: &str) -> Self {
        Self(unsafe_update_view_lifetime_signature(s))
    }
}

impl<'t> FromPyObject<'t> for PyStrView {
    fn extract(ob: &'t PyAny) -> PyResult<Self> {
        Ok(Self(unsafe_update_view_lifetime_signature(ob.extract()?)))
    }
}

impl IntoPy<PyObject> for PyStrView {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl ToPyObject for PyStrView {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}
