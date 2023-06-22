pub mod macros;

pub use self::macros::*;
use pyo3::prelude::*;

/// An zero-cost abstraction for &str for returning &str to Python and accepting &str as the input because
/// receiving &String as input is not possible.
pub struct PyStr(pub &'static str);

impl PyStr {
    pub fn new(s: &str) -> Self {
        Self(unsafe_update_view_lifetime_signature(s))
    }
}

impl<'t> FromPyObject<'t> for PyStr {
    fn extract(ob: &'t PyAny) -> PyResult<Self> {
        Ok(Self(unsafe_update_view_lifetime_signature(ob.extract()?)))
    }
}

impl IntoPy<PyObject> for PyStr {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl ToPyObject for PyStr {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

kgdata::pylist!(list_str_view(
    module = "kgdata.core.pyo3helper",
    name = "ListStrView",
    item = String as kgdata::pyo3helper::PyStr
));

kgdata::pyset!(set_str_view(
    module = "kgdata.core.pyo3helper",
    name = "SetStrView",
    item = String as kgdata::pyo3helper::PyStr
));
