use crate::models::entity::EntityType;
use pyo3::prelude::*;
use pyo3::types::PyString;
use serde::{Deserialize, Serialize};

#[pyclass(module = "kgdata.core.bench")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EntityDesign1 {
    pub id: String,
    pub entity_type: EntityType,
}

#[pymethods]
impl EntityDesign1 {
    #[new]
    fn new(id: String, entity_type: EntityType) -> Self {
        Self { id, entity_type }
    }

    #[staticmethod]
    fn from_bytes(bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes).unwrap()
    }

    #[getter]
    fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    fn entity_type(&self) -> EntityType {
        self.entity_type.clone()
    }
}

#[pyclass(module = "kgdata.core.bench")]
#[derive(Debug, Clone)]
pub struct EntityDesign2 {
    pub id: Py<PyString>,
    pub entity_type: Py<EntityType>,
}

#[pymethods]
impl EntityDesign2 {
    #[new]
    fn new(id: Py<PyString>, entity_type: Py<EntityType>) -> Self {
        Self { id, entity_type }
    }

    #[staticmethod]
    fn from_bytes(py: Python<'_>, bytes: &[u8]) -> Self {
        let ent: EntityDesign1 = serde_json::from_slice(bytes).unwrap();
        let enttype: Py<EntityType> = Py::new(py, ent.entity_type).unwrap();
        Self {
            id: PyString::new(py, &ent.id).into(),
            entity_type: enttype,
        }
    }

    #[getter]
    fn id(&self) -> &Py<PyString> {
        &self.id
    }

    #[getter]
    fn entity_type(&self) -> Py<EntityType> {
        self.entity_type.clone()
    }
}
