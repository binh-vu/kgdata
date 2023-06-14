use super::multilingual::{MultiLingualStringListView, MultiLingualStringView};
use super::value::{value_list_view, ValueView};
use crate::models::{Entity, EntityType, SiteLink, Statement, StatementRank, Value};
use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::{pylist, pymap, pyview};
use pyo3::prelude::*;

pyview!(
    EntityView(module = "kgdata.core.models", name = "EntityView", cls = Entity, derive = (Clone, Debug)) {
        b(id: String),
        b(entity_type: EntityType),
        v(label: MultiLingualStringView),
        v(description: MultiLingualStringView),
        v(aliases: MultiLingualStringListView),
        v(sitelinks: sitelink_map_view::MapView),
        v(props: prop_map_view::MapView),
    }
);

pyview!(
    SiteLinkView(module = "kgdata.core.models", name = "SiteLinkView", cls = SiteLink, derive = (Clone, Debug)) {
        b(site: String),
        b(title: String),
        v(badges: crate::pyo3helper::list_str_view::ListView),
        r(url: Option<&String>),
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

pymap!(sitelink_map_view(
    module = "kgdata.core.models",
    name = "SiteLinkMapView",
    key = String as crate::pyo3helper::PyStr,
    value = super::SiteLink as super::SiteLinkView
));

pylist!(statement_list_view(
    module = "kgdata.core.models",
    name = "StatementListView",
    item = super::Statement as super::StatementView
));

pymap!(prop_map_view(
    module = "kgdata.core.models",
    name = "PropMapView",
    key = String as crate::pyo3helper::PyStr,
    value = Vec<super::Statement> as super::statement_list_view::ListView
));

pymap!(qualifier_map_view(
    module = "kgdata.core.models",
    name = "QualifierMapView",
    key = String as crate::pyo3helper::PyStr,
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
