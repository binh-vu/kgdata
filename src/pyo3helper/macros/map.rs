/// Generating a temporary map view of a Rust's map and an python object that owned the Rust's map. The two structs allows random access at O(1) cost.
///
/// # Placeholder
///
/// * structname: name of the module that will contains the mapview
/// * module: name of the python module that will contains this struct
/// * derivetrait: optional, list of traits to derive for the struct
/// * key: the key type (rust struct)
/// * keyview: the key view type -- assume that we can get the key by calling `keyview.0` and it has a constructor `new(&key)`
/// * value: the value type (rust struct)
/// * valueview: the value view type (a pyo3 class) -- expect to have a constructor `new(&value)`
///
/// # Example
///
/// # TODO: add example
#[macro_export]
macro_rules! pymap {
    ($structname:ident (module = $module:literal, key = $key:ty as $keyview:ty, value = $value:ty as $valueview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;
            use pyo3::types::PyTuple;

            #[pyclass(module = $module, name = "MapView")]
            pub struct MapView(pub &'static hashbrown::HashMap<$key, $value>);

            impl MapView {
                pub fn new(map: &hashbrown::HashMap<$key, $value>) -> Self {
                    Self(unsafe_update_view_lifetime_signature(map))
                }
            }

            #[pymethods]
            impl MapView {
                pub fn __iter__(&self) -> KeysView {
                    KeysView(self.0.keys())
                }

                pub fn keys(&self) -> KeysView {
                    KeysView(self.0.keys())
                }

                pub fn values(&self) -> ValuesView {
                    ValuesView(self.0.values())
                }

                pub fn items(&self) -> ItemsView {
                    ItemsView(self.0.iter())
                }

                pub fn __len__(&self) -> usize {
                    self.0.len()
                }

                fn __getitem__(&self, k: $keyview) -> PyResult<$valueview> {
                    if let Some(v) = self.0.get(k.0) {
                        Ok(<$valueview>::new(v))
                    } else {
                        Err(pyo3::exceptions::PyKeyError::new_err(format!(
                            "Key not found: {}",
                            k.0
                        )))
                    }
                }
            }

            #[pyclass(module = $module, name = "Map")]
            pub struct Map(pub hashbrown::HashMap<$key, $value>);

            impl Map {
                pub fn new(map: hashbrown::HashMap<$key, $value>) -> Self {
                    Self(map)
                }
            }

            #[pymethods]
            impl Map {
                pub fn __iter__(&self) -> KeysView {
                    KeysView(unsafe_update_view_lifetime_signature(&self.0).keys())
                }

                pub fn keys(&self) -> KeysView {
                    KeysView(unsafe_update_view_lifetime_signature(&self.0).keys())
                }

                pub fn values(&self) -> ValuesView {
                    ValuesView(unsafe_update_view_lifetime_signature(&self.0).values())
                }

                pub fn items(&self) -> ItemsView {
                    ItemsView(unsafe_update_view_lifetime_signature(&self.0).iter())
                }

                pub fn __len__(&self) -> usize {
                    self.0.len()
                }

                fn __getitem__(&self, k: $keyview) -> PyResult<$valueview> {
                    if let Some(v) = self.0.get(k.0) {
                        Ok(<$valueview>::new(v))
                    } else {
                        Err(pyo3::exceptions::PyKeyError::new_err(format!(
                            "Key not found: {}",
                            k.0
                        )))
                    }
                }
            }

            #[pyclass]
            pub struct KeysView(pub hashbrown::hash_map::Keys<'static, $key, $value>);
            #[pyclass]
            pub struct ValuesView(pub hashbrown::hash_map::Values<'static, $key, $value>);
            #[pyclass]
            pub struct ItemsView(pub hashbrown::hash_map::Iter<'static, $key, $value>);

            #[pymethods]
            impl KeysView {
                pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                pub fn __next__(&mut self) -> Option<$keyview> {
                    if let Some(k) = self.0.next() {
                        Some(<$keyview>::new(k))
                    } else {
                        None
                    }
                }
            }

            #[pymethods]
            impl ValuesView {
                pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                pub fn __next__(&mut self) -> Option<$valueview> {
                    if let Some(v) = self.0.next() {
                        Some(<$valueview>::new(v))
                    } else {
                        None
                    }
                }
            }

            #[pymethods]
            impl ItemsView {
                pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                pub fn __next__(&mut self, py: Python<'_>) -> Option<Py<PyTuple>> {
                    if let Some((k, v)) = self.0.next() {
                        let output: Py<PyTuple> = (k, <$valueview>::new(v)).into_py(py);
                        Some(output)
                    } else {
                        None
                    }
                }
            }
        }
    };
}
