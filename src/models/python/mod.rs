use pyo3::prelude::*;

pub mod entity;
pub mod iterators;
pub mod value;

/// Change signature of a reference from temporary to static. This is unsafe and
/// only be used for temporary views that drop immediately after use.
fn unsafe_update_view_lifetime_signature<T>(val: &T) -> &'static T {
    let ptr = val as *const T;
    unsafe { &*ptr }
}

pub(crate) fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "models")?;

    m.add_submodule(submodule)?;

    submodule.add_class::<self::entity::PyEntity>()?;
    submodule.add_class::<self::entity::PyStatementView>()?;
    submodule.add_class::<self::value::PyValueView>()?;
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
