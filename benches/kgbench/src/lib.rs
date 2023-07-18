use std::{fs::File, io::BufReader};

use kgdata::{conversions::WDEntity, db::deser_entity};
use pyo3::prelude::*;

use serde_jsonlines::BufReadExt;
use serde_jsonlines::{json_lines, write_json_lines};

#[pyclass]
struct Entities(pub Vec<kgdata::models::Entity>);

#[pyfunction]
fn read_entities(path: &str) -> Entities {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    Entities(
        reader
            .json_lines::<WDEntity>()
            .map(|e| Ok(e?.0))
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap(),
    )
    // Entities(
    //     serde_json::from_reader::<_, Vec<WDEntity>>(reader)
    //         .unwrap()
    //         .into_iter()
    //         .map(|e| e.0)
    //         .collect::<Vec<_>>(),
    // )
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn kgbench(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(read_entities, m)?)?;
    m.add_class::<Entities>()?;
    Ok(())
}
