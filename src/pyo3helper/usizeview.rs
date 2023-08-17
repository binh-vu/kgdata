use pyo3::prelude::*;

/// An zero-cost abstraction for &str for returning &str to Python and accepting &str as the input because
/// receiving &String as input is not possible.
pub struct PyUsizeView(pub usize);

impl PyUsizeView {
    pub fn new(s: &usize) -> Self {
        Self(*s)
    }
}

impl<'t> FromPyObject<'t> for PyUsizeView {
    fn extract(ob: &'t PyAny) -> PyResult<Self> {
        Ok(Self(ob.extract()?))
    }
}

impl IntoPy<PyObject> for PyUsizeView {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

impl ToPyObject for PyUsizeView {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}
