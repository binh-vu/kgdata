use anyhow::Result;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hashbrown::HashMap;
use pyo3::prelude::*;
use serde_json::Value;
use std::{fs, path::Path, rc::Rc};

struct RawEnt {
    id: String,
    props: HashMap<String, Vec<String>>,
}

struct Ent {
    id: Rc<String>,
    props: Rc<HashMap<String, Rc<Vec<String>>>>,
}

struct Ent3 {
    id: String,
    props: HashMap<String, Vec<String>>,
}

// struct Ent2 {
//     id: Py<PyString>,
//     props: Py<HashMap<String, Py<Vec<PyString>>>>,
// }

fn rcdesign(records: &[RawEnt]) -> usize {
    let mut ents = vec![];
    for record in records {
        let props: HashMap<String, Rc<Vec<String>>> = record
            .props
            .iter()
            .map(|(k, v)| (k.to_owned(), Rc::new(v.to_owned())))
            .collect();
        let ent = Ent {
            id: Rc::new(record.id.clone()),
            props: Rc::new(props),
        };
        ents.push(ent);
    }

    let mut count = 0;
    for ent in ents {
        count += ent.id.len();
        for (_, v) in ent.props.iter() {
            count += v.len();
        }
    }
    count
}

fn basedesign(records: &[RawEnt]) -> usize {
    let mut ents = vec![];
    for record in records {
        let props: HashMap<String, Vec<String>> = record
            .props
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let ent = Ent3 {
            id: record.id.clone(),
            props: props,
        };
        ents.push(ent);
    }

    let mut count = 0;
    for ent in ents {
        count += ent.id.len();
        for (_, v) in ent.props.iter() {
            count += v.len();
        }
    }
    count
}

fn pydesign(records: &[RawEnt]) -> usize {
    let mut ents = vec![];

    Python::with_gil(|py| {
        for record in records {
            let props: HashMap<String, Py<Vec<String>>> = record
                .props
                .iter()
                .map(|(k, v)| (k.to_owned(), Py::new(py, v.to_owned()).unwrap()))
                .collect();
            let ent = Ent2 {
                id: Py::new(py, record.id.clone()).unwrap(),
                props: Py::new(py, props).unwrap(),
            };
            ents.push(ent);
        }
    });

    let mut count = 0;
    for ent in ents {
        count += ent.id.len();
        for (_, v) in ent.props.iter() {
            count += v.len();
        }
    }
    count
}

fn criterion_benchmark(c: &mut Criterion) {
    let filename = "wikipedia/List_of_highest_mountains_on_Earth.html";

    let datafile = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("benches/resources")
        .join("wdentities.jl");
    let records: Vec<RawEnt> = fs::read_to_string(datafile)
        .unwrap()
        .split("\n")
        .filter(|s| !s.is_empty())
        .map(|r| {
            let value: Value = serde_json::from_str(r).unwrap();
            let id = value
                .as_object()
                .unwrap()
                .get("id")
                .unwrap()
                .as_str()
                .unwrap();
            let props = value
                .as_object()
                .unwrap()
                .get("props")
                .unwrap()
                .as_object()
                .unwrap()
                .into_iter()
                .map(|(k, v)| {
                    let values: Vec<String> = v
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|v| {
                            v.as_object()
                                .unwrap()
                                .get("rank")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .to_owned()
                        })
                        .collect::<Vec<_>>();
                    (k.to_owned(), values)
                })
                .collect::<HashMap<String, Vec<String>>>();

            RawEnt {
                id: id.to_owned(),
                props,
            }
        })
        .collect();

    let mut group = c.benchmark_group("RCvsPy");

    group.measurement_time(std::time::Duration::from_secs(10));
    group.bench_function("rc", |b| b.iter(|| rcdesign(&records)));
    group.bench_function("base", |b| b.iter(|| basedesign(&records)));
    // group.bench_function("py", |b| b.iter(|| pydesign(&records)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
