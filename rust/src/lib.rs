extern crate self as kgdata;

pub mod conversions;
pub mod db;
pub mod error;
pub mod mapreduce;
pub mod models;
pub mod pyo3helper;
pub mod python;
use pyo3::prelude::*;

#[pyfunction]
pub fn init_env_logger() -> PyResult<()> {
    env_logger::init();
    Ok(())
}

#[cfg(feature = "extension-module")]
#[pymodule]
fn core(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.setattr("__path__", pyo3::types::PyList::empty(py))?;

    m.add_function(wrap_pyfunction!(init_env_logger, m)?)?;
    python::models::register(py, m)?;
    m.add_class::<python::scripts::GetRepresentativeValue>()?;

    Ok(())
}
