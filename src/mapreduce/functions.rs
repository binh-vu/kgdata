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

pub fn make_try_fn<I, O, E, F>(func: F) -> impl Fn(Result<I, E>) -> Result<O, E>
where
    F: Fn(I) -> Result<O, E>,
    E: Send,
{
    move |value| func(value?)
}

pub fn make_try_filter_fn<I, E, F>(func: F) -> impl Fn(&Result<I, E>) -> bool
where
    F: Fn(&I) -> bool,
    E: Send,
{
    move |value| {
        let out = value.as_ref();
        match out {
            Ok(v) => func(v),
            Err(_) => true, // keep the error so we can collect it later
        }
    }
}

pub fn from_jl_files<T>(
    glob: &str,
) -> Result<FlatMapOp<Dataset<PathBuf>, impl Fn(PathBuf) -> Vec<Result<T, KGDataError>>>, KGDataError>
where
    for<'de> T: Deserialize<'de> + Send,
{
    let ds = Dataset::files(glob)?.flat_map(make_try_flat_map_fn(deser_json_lines));
    Ok(ds)
}

fn deser_json_lines<T>(path: PathBuf) -> Result<Vec<T>, KGDataError>
where
    for<'de> T: Deserialize<'de>,
{
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    reader
        .lines()
        .map(|line| serde_json::from_str::<T>(&line?).map_err(|err| err.into()))
        .collect::<Result<Vec<T>, _>>()
}
