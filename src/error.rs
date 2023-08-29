use pyo3::PyErr;
use serde_json;
use thiserror::Error;

/// Represent possible errors returned by this library.
#[derive(Error, Debug)]
pub enum KGDataError {
    #[error("ValueError: {0}")]
    ValueError(String),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SerdeJsonErr(#[from] serde_json::Error),

    #[error(transparent)]
    GlobPatternError(#[from] glob::PatternError),

    #[error(transparent)]
    GlobError(#[from] glob::GlobError),

    #[error(transparent)]
    TryFromSliceErro(#[from] std::array::TryFromSliceError),

    #[error(transparent)]
    RocksDBError(#[from] rocksdb::Error),

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

pub type KGResult<T> = Result<T, KGDataError>;
