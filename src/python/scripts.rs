use std::{fs::File, io::BufReader, path::PathBuf};

use crate::{error::into_pyerr, mapreduce::*};
use hashbrown::HashMap;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::BufRead;

#[pyclass]
pub struct GetRepresentativeValue {
    // dataset: Box<dyn ParallelDataset<Item = PyResult<EntityTypesAndDegrees>>>,
    dataset: Dataset<EntityTypesAndDegrees>,
}

#[pymethods]
impl GetRepresentativeValue {
    #[new]
    pub fn new(data_dir: &str) -> PyResult<Self> {
        // let ds = Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //     .map_err(into_pyerr)?
        //     .flat_map(test);
        // let vec: Vec<EntityTypesAndDegrees> = ds.collect();
        let ds = Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
            .map_err(into_pyerr)?
            .flat_map(test2);
        let vec: Result<Vec<EntityTypesAndDegrees>, _> = ds.collect();
        unimplemented!()
        // Ok(Self {
        //     dataset: Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //         .map_err(into_pyerr)?
        //         .flat_map(deser_ent_types)
        //         .collect::<PyResult<_>>()?,
        // })
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

fn test(path: PathBuf) -> Vec<EntityTypesAndDegrees> {
    deser_ent_types(path).unwrap()
}

fn test2(path: PathBuf) -> Result<Vec<EntityTypesAndDegrees>, String> {
    Ok(deser_ent_types(path).unwrap())
}

fn deser_ent_types(path: PathBuf) -> PyResult<Vec<EntityTypesAndDegrees>> {
    let mut file = File::open(path).map_err(into_pyerr)?;
    let reader = BufReader::new(file);

    reader
        .lines()
        .map(|line| {
            serde_json::from_str::<EntityTypesAndDegrees>(&line.map_err(into_pyerr)?)
                .map_err(into_pyerr)
        })
        .collect::<PyResult<_>>()
}
