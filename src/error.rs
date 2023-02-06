use pyo3::PyErr;
use serde_json;
use thiserror::Error;

/// Represent possible errors returned by this library.
#[derive(Error, Debug)]
pub enum KGDataError {
    /// serde_json error
    #[error(transparent)]
    SerdeJsonErr(#[from] serde_json::Error),

    /// PyO3 error
    #[error(transparent)]
    PyErr(#[from] pyo3::PyErr),
}

pub fn into_pyerr<E: Into<KGDataError>>(err: E) -> PyErr {
    let hderr = err.into();
    if let KGDataError::PyErr(e) = hderr {
        e
    } else {
        let anyerror: anyhow::Error = hderr.into();
        anyerror.into()
    }
}
