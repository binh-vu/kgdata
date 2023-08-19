use std::{fs::File, io::BufReader, path::PathBuf};

use crate::models::MultiLingualString;
use crate::{error::into_pyerr, mapreduce::*};
use hashbrown::{HashMap, HashSet};
use pyo3::prelude::*;
use rayon::prelude::IntoParallelIterator;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::BufRead;

#[pyclass]
pub struct GetRepresentativeValue {
    types_and_degrees: Dataset<EntityTypesAndDegrees>,
    id2labels: HashMap<String, EntityLabel>,
}

#[pymethods]
impl GetRepresentativeValue {
    #[new]
    pub fn new(data_dir: &str, class_ids: Vec<String>) -> PyResult<Self> {
        let filtered_ids = HashSet::from(class_ids);
        // let ds = Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //     .map_err(into_pyerr)?
        //     .flat_map(test);
        // let vec: Vec<EntityTypesAndDegrees> = ds.collect();
        // let ds = Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        //     .map_err(into_pyerr)?
        //     .flat_map(deser_ent_types)
        //     .collect();
        // // .map(test4);
        // // let vec: Result<Vec<EntityTypesAndDegrees>, usize> = ds.collect();
        // // let vec2 = Result::<Vec<EntityTypesAndDegrees>, usize>::from_par_dataset(ds);
        // // let ds = Dataset::files(&format!("{}/entity_types_and_degrees/*.gz", data_dir))
        // //     .map_err(into_pyerr)?
        // //     .map(test3);
        // // let vec: PyResult<Vec<EntityTypesAndDegrees>> = ds.collect();
        // unimplemented!()
        let types_and_degrees = from_jl_files::<EntityTypesAndDegrees>(&format!(
            "{}/entity_types_and_degrees/*.gz",
            data_dir
        ));
        // Ok(Self {
        //     types_and_degrees: Dataset::files(&format!(
        //         "{}/entity_types_and_degrees/*.gz",
        //         data_dir
        //     ))
        //     .map_err(into_pyerr)?
        //     .flat_map(make_try_flat_map_fn(deser_ent_types))
        //     .collect::<PyResult<Dataset<_>>>()?,
        // })
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
