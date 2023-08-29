use crate::error::KGResult;
use crate::models::MultiLingualString;
use crate::{error::into_pyerr, mapreduce::*};
use hashbrown::{HashMap, HashSet};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use serde::{Deserialize, Serialize};

#[pyclass]
pub struct GetRepresentativeValue {
    pub data_dir: String,
    pub class_ids: HashSet<String>,
    pub kgname: String,
    pub topk: usize,
}

impl GetRepresentativeValue {
    pub fn get_score(&self, ent: &EntityTypesAndDegrees, class_id: &str) -> f32 {
        let outscale = 0.95;
        let inscale = 1.0 - outscale;
        let degree_scale = 0.1;

        let wp_score = ent.wikipedia_indegree.unwrap_or(0) as f32 * inscale
            + ent.wikipedia_outdegree.unwrap_or(0) as f32 * outscale;

        let db_score = ent.indegree as f32 * inscale + ent.outdegree as f32 * outscale;

        let dist_score = match ent.types[class_id] {
            0 => 0.0,
            1 => -500.0,
            2 => -10000.0,
            3 => -30000.0,
            dist => -(dist as f32) * 20000.0,
        };

        (wp_score + db_score) * degree_scale + dist_score
    }

    pub fn get_ent_types_and_degrees_files(&self) -> String {
        format!("{}/entity_types_and_degrees/*.gz", self.data_dir)
    }

    pub fn get_ent_label_files(&self) -> String {
        format!("{}/entity_labels/*.gz", self.data_dir)
    }
}

#[pymethods]
impl GetRepresentativeValue {
    #[new]
    pub fn new(data_dir: String, class_ids: HashSet<String>, kgname: String, topk: usize) -> Self {
        Self {
            data_dir,
            class_ids,
            kgname,
            topk,
        }
    }

    pub fn get_examples<'t>(&self, py: Python<'t>) -> PyResult<&'t PyDict> {
        let matched_ents =
            from_jl_files::<EntityTypesAndDegrees>(&self.get_ent_types_and_degrees_files())
                .map_err(into_pyerr)?
                .filter(make_try_filter_fn(|x: &EntityTypesAndDegrees| {
                    x.types.iter().any(|cid| self.class_ids.contains(cid.0))
                }))
                .collect::<KGResult<Vec<_>>>()
                .map_err(into_pyerr)?;

        let type2examples = RefDataset::new(&matched_ents)
            .flat_map(|ent| {
                ent.types
                    .keys()
                    .filter_map(|cid| {
                        if self.class_ids.contains(cid) {
                            Some((cid, ent))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .group_by_map(|item| item.0, |item| item.1)
            .map(|item| {
                let mut newents = item
                    .1
                    .into_iter()
                    .map(|ent| (ent, self.get_score(ent, item.0)))
                    .collect::<Vec<_>>();
                newents.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                newents.truncate(self.topk);
                (item.0, newents.into_iter().map(|x| x.0).collect::<Vec<_>>())
            })
            .collect::<HashMap<_, _>>();

        let matched_ent_ids = matched_ents
            .iter()
            .map(|ent| &ent.id)
            .collect::<HashSet<_>>();

        let matched_ent_labels = from_jl_files::<EntityLabel>(&self.get_ent_label_files())
            .map_err(into_pyerr)?
            .filter(make_try_filter_fn(|x: &EntityLabel| {
                matched_ent_ids.contains(&x.id)
            }))
            .map(make_try_fn(|x: EntityLabel| Ok((x.id.clone(), x))))
            .collect::<KGResult<HashMap<_, _>>>()
            .map_err(into_pyerr)?;

        let output = PyDict::new(py);

        for cid in &self.class_ids {
            output.set_item(
                cid,
                type2examples[cid]
                    .iter()
                    .map(|ent| {
                        let dict = PyDict::new(py);
                        dict.set_item("id", &ent.id)?;
                        dict.set_item(
                            "label",
                            multi_lingual_string_to_dict(py, &matched_ent_labels[&ent.id].label)?,
                        )?;
                        dict.set_item("score", self.get_score(ent, cid))?;
                        Ok(dict)
                    })
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
        }

        Ok(output)
    }

    /// Calculate the number of entities for each type. This is useful when we want to determine
    /// what types we should group together in one pass. Some big types that require more memory
    /// should run alone.
    pub fn calculate_stats<'t>(&self, py: Python<'t>) -> PyResult<&'t PyDict> {
        let type_counts =
            from_jl_files::<EntityTypesAndDegrees>(&self.get_ent_types_and_degrees_files())
                .map_err(into_pyerr)?
                .flat_map(make_try_flat_map_fn(|x: EntityTypesAndDegrees| {
                    Ok(self.class_ids.iter().filter_map(move |cid| {
                        if x.types.contains_key(cid) {
                            Some(cid.as_str())
                        } else {
                            None
                        }
                    }))
                }))
                .fold(
                    || Ok(HashMap::new()),
                    |map: KGResult<HashMap<&str, usize>>, item: KGResult<&str>| {
                        let mut map = map?;
                        let item = item?;
                        if map.contains_key(item) {
                            *map.get_mut(item).unwrap() += 1;
                        } else {
                            map.insert(item, 1);
                        }
                        Ok(map)
                    },
                )
                .reduce(
                    || Ok(HashMap::new()),
                    |map: KGResult<HashMap<&str, usize>>, map2: KGResult<HashMap<&str, usize>>| {
                        let mut map = map?;
                        for (k, v) in map2?.into_iter() {
                            if map.contains_key(&k) {
                                *map.get_mut(&k).unwrap() += v;
                            } else {
                                map.insert(k, v);
                            }
                        }
                        Ok(map)
                    },
                )
                .map_err(into_pyerr)?;

        Ok(type_counts.into_py_dict(py))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EntityTypesAndDegrees {
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

fn multi_lingual_string_to_dict<'t>(
    py: Python<'t>,
    label: &MultiLingualString,
) -> PyResult<&'t PyDict> {
    let dict = PyDict::new(py);
    dict.set_item("lang2value", (&label.lang2value).into_py_dict(py))?;
    dict.set_item("lang", &label.lang)?;
    Ok(dict)
}
