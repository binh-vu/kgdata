use pyo3::prelude::*;

use crate::models::DataType;

// use crate::models::MultiLingualString;

// impl<'t> FromPyObject<'t> for MultiLingualString {
//     fn extract(ob: &'t PyAny) -> PyResult<Self> {
//         Ok(Self(unsafe_update_view_lifetime_signature(ob.extract()?)))
//     }
// }

impl IntoPy<PyObject> for &DataType {
    fn into_py(self, py: Python) -> PyObject {
        self.to_str().into_py(py)
    }
}
