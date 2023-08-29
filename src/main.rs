use anyhow::Result;
use hashbrown::HashSet;
use kgdata::mapreduce::*;
use kgdata::python::scripts::GetRepresentativeValue;
use kgdata::{error::into_pyerr, mapreduce::from_jl_files, python::scripts::EntityTypesAndDegrees};
use pyo3::prelude::*;

fn main() -> PyResult<()> {
    let args = GetRepresentativeValue {
        data_dir: "/Volumes/research/kgdata/data/dbpedia/20221201".to_string(),
        class_ids: HashSet::from_iter(vec!["http://dbpedia.org/ontology/Person".to_string()]),
        kgname: "dbpedia".to_string(),
        topk: 1000,
    };

    // Python::with_gil(|py| {
    //     let res = GetRepresentativeValue::calculate_stats(py, &args).unwrap();
    //     println!("{:?}", res);
    // });

    println!("Hello, world!");
    Ok(())
}
