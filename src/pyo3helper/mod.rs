pub mod hashbrown;
pub mod macros;
pub mod strview;
pub mod usizeview;

pub use self::hashbrown::*;
pub use self::macros::*;
pub use self::strview::*;
pub use self::usizeview::*;

kgdata::pylist!(list_str_view(
    module = "kgdata.core.pyo3helper",
    item = String as kgdata::pyo3helper::PyStrView
));

kgdata::pyset!(set_str_view(
    module = "kgdata.core.pyo3helper",
    item = String as kgdata::pyo3helper::PyStrView
));

kgdata::pymap!(map_str_view(
    module = "kgdata.core.pyo3helper",
    key = String as kgdata::pyo3helper::PyStrView,
    value = String as kgdata::pyo3helper::PyStrView
));

kgdata::pymap!(map_usize_view(
    module = "kgdata.core.pyo3helper",
    key = String as kgdata::pyo3helper::PyStrView,
    value = usize as kgdata::pyo3helper::PyUsizeView
));
