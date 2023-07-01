pub mod hashbrown;
pub mod macros;
pub mod strview;

pub use self::hashbrown::*;
pub use self::macros::*;
pub use self::strview::*;

kgdata::pylist!(list_str_view(
    module = "kgdata.core.pyo3helper",
    name = "ListStrView",
    item = String as kgdata::pyo3helper::PyStrView
));

kgdata::pyset!(set_str_view(
    module = "kgdata.core.pyo3helper",
    name = "SetStrView",
    item = String as kgdata::pyo3helper::PyStrView
));
