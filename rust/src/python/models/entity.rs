use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use super::value::{value_list_view, ValueView};
use crate::conversions::WDEntity;
use crate::error::into_pyerr;
use crate::models::{Entity, EntityType, Statement, StatementRank, Value};
use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::{pylist, pymap, pyview, pywrap};
use pyo3::prelude::*;

pyview!(
    EntityView(module = "kgdata.core.models", name = "EntityView", cls = Entity, derive = (Clone, Debug)) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(props: prop_map_view::MapView),
    }
);
pywrap!(
    PyEntity(module = "kgdata.core.models", name = "Entity", cls = Entity) {
        b(id: String),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(props: prop_map_view::MapView),
    }
);

pyview!(
    StatementView(module = "kgdata.core.models", name = "StatementView", cls = Statement) {
        v(value: ValueView),
        v(qualifiers: qualifier_map_view::MapView),
        v(qualifiers_order: crate::pyo3helper::list_str_view::ListView),
        b(rank: StatementRank),
    }
);

pylist!(statement_list_view(
    module = "kgdata.core.models",
    item = super::Statement as super::StatementView
));

pymap!(prop_map_view(
    module = "kgdata.core.models",
    key = String as crate::pyo3helper::PyStrView,
    value = Vec<super::Statement> as super::statement_list_view::ListView
));

pymap!(qualifier_map_view(
    module = "kgdata.core.models",
    key = String as crate::pyo3helper::PyStrView,
    value = Vec<super::Value> as super::value_list_view::ListView
));

impl IntoPy<PyObject> for &EntityType {
    fn into_py(self, py: Python) -> PyObject {
        self.to_str().into_py(py)
    }
}

impl IntoPy<PyObject> for &StatementRank {
    fn into_py(self, py: Python) -> PyObject {
        self.to_str().into_py(py)
    }
}

#[pymethods]
impl PyEntity {
    // Create a new Entity from a JSON string.
    #[staticmethod]
    fn from_wdentity_json(data: &[u8]) -> PyResult<Self> {
        let ent = serde_json::from_slice::<WDEntity>(data)
            .map_err(into_pyerr)?
            .0;
        return Ok(PyEntity(ent));
    }
}
