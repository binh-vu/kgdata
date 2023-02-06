use pyo3::prelude::*;

pub mod entity_design;

use self::entity_design::*;

pub(crate) fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "bench")?;

    m.add_submodule(submodule)?;

    submodule.add_class::<EntityDesign1>()?;
    submodule.add_class::<EntityDesign2>()?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("kgdata.kgdata.bench", submodule)?;

    Ok(())
}
