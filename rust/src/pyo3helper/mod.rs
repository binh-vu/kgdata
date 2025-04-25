pub mod hashbrown;
pub mod macros;
pub mod strview;
pub mod usizeview;

pub use self::hashbrown::*;
pub use self::macros::*;
pub use self::strview::*;
pub use self::usizeview::*;

kgdata_core::pylist!(list_str_view(
    module = "kgdata_core.pyo3helper",
    item = String as kgdata_core::pyo3helper::PyStrView
));

kgdata_core::pyset!(set_str_view(
    module = "kgdata_core.pyo3helper",
    item = String as kgdata_core::pyo3helper::PyStrView
));

kgdata_core::pymap!(map_str_view(
    module = "kgdata_core.pyo3helper",
    key = String as kgdata_core::pyo3helper::PyStrView,
    value = String as kgdata_core::pyo3helper::PyStrView
));

kgdata_core::pymap!(map_usize_view(
    module = "kgdata_core.pyo3helper",
    key = String as kgdata_core::pyo3helper::PyStrView,
    value = usize as kgdata_core::pyo3helper::PyUsizeView
));
