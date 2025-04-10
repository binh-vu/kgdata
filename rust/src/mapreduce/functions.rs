use std::{ffi::OsStr, fs::File, io::BufRead, io::BufReader, path::PathBuf};

use flate2::read::GzDecoder;
use serde::Deserialize;

use crate::error::KGDataError;

use super::*;

pub fn make_begin_try_flat_map_fn<I, E, F, OPI>(func: F) -> impl Fn(I) -> Vec<Result<OPI::Item, E>>
where
    F: Fn(I) -> Result<OPI, E>,
    OPI: IntoIterator,
    E: Send,
{
    move |value| {
        let out = func(value);
        match out {
            Ok(v) => v.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(e) => vec![Err(e)],
        }
    }
}

pub fn make_try_flat_map_fn<I, E, F, OPI>(
    func: F,
) -> impl Fn(Result<I, E>) -> Vec<Result<OPI::Item, E>>
where
    F: Fn(I) -> Result<OPI, E>,
    OPI: IntoIterator,
    E: Send,
{
    move |value| match value {
        Ok(value) => {
            let out = func(value);
            match out {
                Ok(v) => v.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        }
        Err(e) => vec![Err(e)],
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
    let ds = Dataset::files(glob)?.flat_map(make_begin_try_flat_map_fn(deser_json_lines));
    Ok(ds)
}

pub fn deser_json_lines<T>(path: PathBuf) -> Result<Vec<T>, KGDataError>
where
    for<'de> T: Deserialize<'de>,
{
    let ext = path.extension().and_then(OsStr::to_str).map(str::to_owned);
    let file = File::open(path)?;

    let reader: Box<dyn BufRead> = if let Some(ext) = ext {
        match ext.as_str() {
            "gz" => Box::new(BufReader::new(GzDecoder::new(file))),
            _ => unimplemented!(),
        }
    } else {
        Box::new(BufReader::new(file))
    };
    reader
        .lines()
        .map(|line| serde_json::from_str::<T>(&line?).map_err(|err| err.into()))
        .collect::<Result<Vec<T>, _>>()
}

pub fn merge_map_list<K, V>(
    mut map: HashMap<K, Vec<V>>,
    another: HashMap<K, Vec<V>>,
) -> HashMap<K, Vec<V>>
where
    K: Hash + Eq,
{
    for (k, v) in another.into_iter() {
        match map.get_mut(&k) {
            Some(lst) => lst.extend(v),
            None => {
                map.insert(k, v);
            }
        }
    }
    map
}
