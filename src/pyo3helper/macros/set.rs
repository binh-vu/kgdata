/// Generating a temporary set view of a Rust's set. This view allows random access at O(1) cost.
///
/// # Placeholder
///
/// * structname: name of the module that will contains the mapview
/// * module: name of the python module that will contains this struct
/// * derivetrait: optional, list of traits to derive for the struct
/// * itemview: the item view type (a pyo3 class) -- expect to have a constructor `new(&item)`
/// * item: the item type (rust struct)
///
/// # Example
///
/// # TODO: add example
#[macro_export]
macro_rules! pyset {
    ($structname:ident (module = $module:literal, item = $item:ty as $itemview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;

            #[pyclass(module = $module, name = "SetView")]
            pub struct SetView(pub &'static hashbrown::HashSet<$item>);
            #[pyclass]
            pub struct IterView(pub hashbrown::hash_set::Iter<'static, $item>);

            impl SetView {
                pub fn new(lst: &hashbrown::HashSet<$item>) -> Self {
                    Self(unsafe_update_view_lifetime_signature(lst))
                }
            }

            #[pymethods]
            impl SetView {
                fn __iter__(&self) -> IterView {
                    IterView(self.0.iter())
                }

                fn __len__(&self) -> usize {
                    self.0.len()
                }

                fn __contains__(&self, item: $itemview) -> bool {
                    self.0.contains(item.0)
                }
            }

            #[pymethods]
            impl IterView {
                pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                pub fn __next__(&mut self) -> Option<$itemview> {
                    if let Some(v) = self.0.next() {
                        Some(<$itemview>::new(v))
                    } else {
                        None
                    }
                }
            }
        }
    };
}
