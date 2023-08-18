use std::{fs::File, path::PathBuf};

use crate::mapreduce::*;
use hashbrown::HashMap;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

#[pyclass]
pub struct GetRepresentativeValue {
    // dataset: Box<dyn ParallelDataset<Item = PyResult<EntityTypesAndDegrees>>>,
}

#[pymethods]
impl GetRepresentativeValue {
    #[new]
    pub fn new(data_dir: &str) -> PyResult<Self> {
        // let ds: Box<dyn ParallelDataset<Item = EntityTypesAndDegrees>> = Box::new(
        //     Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //         .unwrap()
        //         .flat_map(|path| serde_json::from_reader(File::open(path).unwrap()).unwrap()),
        // );
        // let _ds: MapOp<Dataset<PathBuf>, fn(PathBuf) -> EntityTypesAndDegrees> =
        //     Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //         .unwrap()
        //         .map(deser_ent_types);

        // let dataset: Box<dyn ParallelDataset<Item = EntityTypesAndDegrees>> = Box::new(
        //     Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //         .unwrap()
        //         .map(deser_ent_types),
        // );

        // let ds: FlatMapOp<Dataset<PathBuf>, dyn Fn<(PathBuf), EntityTypesAndDegrees>> =
        //     Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //         .unwrap()
        // .flat_map(|path| serde_json::from_reader(File::open(path).unwrap()).unwrap());
        // Ok(Self { dataset: ds })
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

fn deser_ent_types(path: PathBuf) -> EntityTypesAndDegrees {
    serde_json::from_reader(File::open(path).unwrap()).unwrap()
}
