use pyo3::PyErr;
use serde_json;
use thiserror::Error;

/// Represent possible errors returned by this library.
#[derive(Error, Debug)]
pub enum KGDataError {
    #[error("KeyError: {0}")]
    KeyError(String),

    #[error("ValueError: {0}")]
    ValueError(String),

    #[error("TimeoutError: {0}")]
    TimeoutError(String),

    #[error("InterruptedError: {0}")]
    InterruptedError(&'static str),

    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    SerdeJsonErr(#[from] serde_json::Error),

    #[error(transparent)]
    GlobPatternError(#[from] glob::PatternError),

    #[error(transparent)]
    GlobError(#[from] glob::GlobError),

    #[error(transparent)]
    TryFromSliceError(#[from] std::array::TryFromSliceError),

    #[error(transparent)]
    RocksDBError(#[from] rocksdb::Error),

    #[error(transparent)]
    PyErr(#[from] pyo3::PyErr),

    #[error("NNG (Messaging Library) Error: {0}")]
    NNGError(#[from] nng::Error),

    /// Error due to incorrect NNG's usage
    #[error("IPC Impl Error: {0}")]
    IPCImplError(String),

    /// Error due to shared memory usage
    #[error("Shared Memory Error: {0}")]
    SharedMemoryError(String),
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
