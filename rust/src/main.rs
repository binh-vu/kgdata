use anyhow::Result;
use clap::Parser;
use kgdata::db::{serve_db, PredefinedDB};
use kgdata::error::KGDataError;
use std::ffi::OsStr;

/// Read some lines of a file
#[derive(Debug, Parser)]
struct ServerCLI {
    /// name of the database
    db: PredefinedDB,
    /// url to serve the database
    url: String,
    /// path to the parent directory of the database
    datadir: String,
}

impl ServerCLI {
    /// Start and serve a DB server at the given URL.
    fn start(&self) -> Result<(), KGDataError> {
        let db = self.db.open_raw_db(OsStr::new(&self.datadir))?;
        serve_db(&self.url, &db)
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let cli = ServerCLI::parse();
    cli.start()?;

    Ok(())
}
