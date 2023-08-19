use std::{fs::File, io::BufRead, io::BufReader, path::PathBuf};

use rayon::prelude::*;
use serde::Deserialize;

use crate::error::KGDataError;

use super::*;

pub fn make_try_flat_map_fn<T, E, F, OPI>(func: F) -> impl Fn(T) -> Vec<Result<OPI::Item, E>>
where
    F: Fn(T) -> Result<OPI, E>,
    OPI: IntoParallelIterator,
    E: Send,
{
    move |value| {
        let out = func(value);
        match out {
            Ok(v) => v.into_par_iter().map(Ok).collect::<Vec<_>>(),
            Err(e) => vec![Err(e)],
        }
    }
}

pub fn from_jl_files<'a, T: 'a>(
    glob: &str,
) -> Result<
    FlatMapOp<Dataset<PathBuf>, impl Fn(PathBuf) -> Vec<Result<T, KGDataError>> + 'a>,
    KGDataError,
>
where
    // F: Fn(PathBuf) -> Result<Vec<T>, KGDataError>,
    T: Deserialize<'a> + Send,
{
    let ds = Dataset::files(glob)?.flat_map(make_try_flat_map_fn(deser_json_lines));

    Ok(ds)
}

fn deser_json_lines<'a, T: 'a>(path: PathBuf) -> Result<Vec<T>, KGDataError>
where
    T: Deserialize<'a>,
{
    let file = File::open(path)?;
    BufReader::new(file)
        .lines()
        .map(|line| serde_json::from_str::<T>(&line?).map_err(|err| err.into()))
        .collect::<Result<Vec<T>, _>>()
}
