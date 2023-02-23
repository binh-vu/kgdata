pub mod conversions;
pub mod db;
pub mod error;
pub mod models;

use pyo3::{prelude::*, types::PyList};

#[pyfunction]
pub fn init_env_logger() -> PyResult<()> {
    env_logger::init();
    Ok(())
}

#[pymodule]
fn core(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.setattr("__path__", PyList::empty(py))?;

    m.add_function(wrap_pyfunction!(init_env_logger, m)?)?;
    models::python::register(py, m)?;

    Ok(())
}
