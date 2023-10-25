use anyhow::Result;
// use hashbrown::HashSet;
// use kgdata::mapreduce::*;
use clap::Parser;
use kgdata::db::{serve_db, KGDB};
use kgdata::error::KGDataError;
use std::path::PathBuf;
// use kgdata::python::scripts::GetRepresentativeValue;
// use kgdata::{error::into_pyerr, mapreduce::from_jl_files, python::scripts::EntityTypesAndDegrees};

/// Read some lines of a file
#[derive(Debug, Parser)]
struct ServerCLI {
    /// name of the database
    dbname: String,
    /// url to serve the database
    url: String,
    /// path to the parent directory of the database
    datadir: String,
}

impl ServerCLI {
    /// Start and serve a DB server at the given URL.
    fn start(&self) -> Result<(), KGDataError> {
        let datadir = PathBuf::from(&self.datadir);
        let db = match self.dbname.as_ref() {
            "entity" => KGDB::open_entity_raw_db(datadir)?,
            "entity_metadata" => KGDB::open_entity_metadata_raw_db(datadir)?,
            "entity_redirection" => KGDB::open_entity_redirection_raw_db(datadir)?,
            _ => panic!("Unknown database name: {}", self.dbname),
        };
        serve_db(&self.url, &db)
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let cli = ServerCLI::parse();
    cli.start()?;

    // let args = GetRepresentativeValue {
    //     data_dir: "/Volumes/research/kgdata/data/dbpedia/20221201".to_string(),
    //     class_ids: HashSet::from_iter(vec!["http://dbpedia.org/ontology/Person".to_string()]),
    //     kgname: "dbpedia".to_string(),
    //     topk: 1000,
    // };

    // // Python::with_gil(|py| {
    // //     let res = GetRepresentativeValue::calculate_stats(py, &args).unwrap();
    // //     println!("{:?}", res);
    // // });

    // println!("Hello, world!");
    Ok(())
}
