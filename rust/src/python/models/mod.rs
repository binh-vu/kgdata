use pyo3::prelude::*;

pub mod class;
pub mod conversions;
pub mod entity;
pub mod entity_metadata;
pub mod multilingual;
pub mod property;
pub mod value;

pub use self::class::*;
pub use self::entity::*;
pub use self::entity_metadata::*;
pub use self::multilingual::*;
pub use self::property::*;
pub use self::value::*;

#[allow(dead_code)]
pub(crate) fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "models")?;

    m.add_submodule(submodule)?;

    submodule.add_class::<self::entity::EntityView>()?;
    submodule.add_class::<self::entity::PyEntity>()?;
    submodule.add_class::<self::entity::StatementView>()?;
    submodule.add_class::<self::entity_metadata::PyEntityMetadata>()?;
    submodule.add_class::<self::property::PyProperty>()?;
    submodule.add_class::<self::class::PyClass>()?;
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
