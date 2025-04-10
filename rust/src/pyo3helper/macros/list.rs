/// Generating a temporary iterator over an array that allow random access at O(1) cost once the iterator is still valid.
///
/// # Placeholder
///
/// * structname: name of the generated struct
/// * module: name of the python module that will contains this struct
/// * derivetrait: optional, list of traits to derive for the struct
/// * itemview: the item view type (a pyo3 class) -- expect to have a constructor `new(&item)`
/// * item: the item type (rust struct)
///
/// # Example
///
/// ```ignore
/// use kgdata::pyo3helper::PyStr;
///
/// pylist!(ListStrView (module = "kgdata.core.test", derive = (Clone, Debug)) {
///     String: PyStr,
/// });
/// ```
/// The above macro will generate the following code:
///
/// ```ignore
/// pub mod ListStrView {
///     use pyo3::prelude::*;
///     use kgdata::pyo3helper::PyStr;
///
///     #[pyclass(module = "kgdata.core.test", name = "ListView")]
///     #[derive(Clone, Debug)]
///     pub struct ListView(pub &'static [String]);
///
///     #[pyclass]
///     pub struct IterView(pub std::slice::Iter<'static, String>);
///
///     #[pymethods]
///     impl ListView {
///         fn new(lst: &[String]) -> Self;
///         fn __len__(&self) -> usize;
///         fn __iter__(&self) -> IterView;
///         fn __getitem__(&self, i: usize) -> PyResult<PyStr>;
///     }
///
///     #[pymethods]
///     impl IterView {
///         fn __iter__(&self) -> Self;
///         fn __next__(&mut self) -> Option<PyStr>;
///     }
///
/// }
/// ```
#[macro_export]
macro_rules! pylist {
    ($structname:ident (module = $module:literal, item = $item:ty as $itemview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;

            #[pyclass(module = $module, name = "ListView")]
            pub struct ListView(pub &'static [$item]);
            #[pyclass]
            pub struct IterView(pub std::slice::Iter<'static, $item>);

            impl ListView {
                pub fn new(lst: &[$item]) -> Self {
                    Self(unsafe_update_view_lifetime_signature(lst))
                }
            }

            #[pymethods]
            impl ListView {
                fn __iter__(&self) -> IterView {
                    IterView(self.0.iter())
                }

                fn __len__(&self) -> usize {
                    self.0.len()
                }

                fn __getitem__(&self, i: usize) -> PyResult<$itemview> {
                    if i < self.0.len() {
                        Ok(<$itemview>::new(&self.0[i]))
                    } else {
                        Err(pyo3::exceptions::PyIndexError::new_err(format!(
                            "index {} is out of range",
                            i
                        )))
                    }
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

            #[pyclass(module = $module, name = "List")]
            pub struct List(pub Vec<$item>);

            impl List {
                pub fn new(lst: Vec<$item>) -> Self {
                    Self(lst)
                }
            }

            #[pymethods]
            impl List {
                fn __iter__(&self) -> IterView {
                    IterView(unsafe_update_view_lifetime_signature(&self.0).iter())
                }

                fn __len__(&self) -> usize {
                    self.0.len()
                }

                fn __getitem__(&self, i: usize) -> PyResult<$itemview> {
                    if i < self.0.len() {
                        Ok(<$itemview>::new(&self.0[i]))
                    } else {
                        Err(pyo3::exceptions::PyIndexError::new_err(format!(
                            "index {} is out of range",
                            i
                        )))
                    }
                }
            }
        }
    };
}
