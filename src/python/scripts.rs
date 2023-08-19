use std::{fs::File, io::BufReader, path::PathBuf};

use crate::models::MultiLingualString;
use crate::{error::into_pyerr, mapreduce::*};
use hashbrown::{HashMap, HashSet};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::BufRead;

#[pyclass]
pub struct GetRepresentativeValue {
    types_and_degrees: HashMap<String, EntityTypesAndDegrees>,
    id2labels: HashMap<String, EntityLabel>,
}

#[pymethods]
impl GetRepresentativeValue {
    #[new]
    pub fn new(data_dir: &str, class_ids: Vec<String>, kgname: &str) -> PyResult<Self> {
        let filtered_ids: HashSet<String> = HashSet::from_iter(class_ids);
        let types_and_degrees = from_jl_files::<EntityTypesAndDegrees>(&format!(
            "{}/entity_types_and_degrees/*.gz",
            data_dir
        ))
        .map_err(into_pyerr)?
        .filter(make_try_filter_fn(|x: &EntityTypesAndDegrees| {
            !filtered_ids.is_disjoint(&x.types.keys().cloned().collect::<HashSet<_>>())
        }))
        .map(make_try_fn(|x: EntityTypesAndDegrees| {
            Ok((x.id.clone(), x))
        }))
        .collect::<Result<Vec<_>, _>>()
        .map_err(into_pyerr)?;

        let types_and_degrees =
            HashMap::<String, EntityTypesAndDegrees>::from_iter(types_and_degrees);

        unimplemented!()
    }

    // pub fn __call__(&self, class_ids: Vec<String>, k: usize) -> Result<Py<PyDict>, KGDataError> {

    //     Ok(())
    // }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EntityTypesAndDegrees {
    id: String,

    types: HashMap<String, usize>,

    indegree: usize,
    outdegree: usize,

    wikipedia_indegree: Option<usize>,
    wikipedia_outdegree: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EntityLabel {
    id: String,
    label: MultiLingualString,
}

fn deser_ent_types(path: PathBuf) -> PyResult<Vec<EntityTypesAndDegrees>> {
    let file = File::open(path).map_err(into_pyerr)?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .map(|line| {
            serde_json::from_str::<EntityTypesAndDegrees>(&line.map_err(into_pyerr)?)
                .map_err(into_pyerr)
        })
        .collect::<PyResult<Vec<EntityTypesAndDegrees>>>()
}
