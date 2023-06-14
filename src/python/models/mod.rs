use pyo3::prelude::*;

pub mod entity;
// pub mod iterators;
pub mod multilingual;
pub mod value;

#[allow(dead_code)]
pub(crate) fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "models")?;

    m.add_submodule(submodule)?;

    submodule.add_class::<self::entity::EntityView>()?;
    submodule.add_class::<self::entity::StatementView>()?;
    submodule.add_class::<self::entity::SiteLinkView>()?;
    submodule.add_class::<self::value::ValueView>()?;
    submodule.add_class::<self::value::PyValue>()?;
    submodule.add_class::<self::value::PyEntityId>()?;
    submodule.add_class::<self::value::PyTime>()?;
    submodule.add_class::<self::value::PyQuantity>()?;
    submodule.add_class::<self::value::PyGlobeCoordinate>()?;
    submodule.add_class::<self::value::PyMonolingualText>()?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("kgdata.core.models", submodule)?;

    Ok(())
}