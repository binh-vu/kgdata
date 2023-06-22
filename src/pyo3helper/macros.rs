/// Generating a temporary iterator over an array that allow random access at O(1) cost once the iterator is still valid.
///
/// # Placeholder
///
/// * clsname: name of the generated struct
/// * module: name of the python module that will contains this struct
/// * name: name of the python class in the module
/// * derivetrait: optional, list of traits to derive for the struct
/// * itemview: the item view type (a pyo3 class) -- expect to have a constructor `new(&item)`
/// * item: the item type (rust struct)
///
/// # Example
///
/// ```ignore
/// use kgdata::pyo3helper::PyStr;
///
/// pylist!(ListStrView (module = "kgdata.core.test", name = "ListStrView", derive = (Clone, Debug)) {
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
///     #[pyclass(module = "kgdata.core.test", name = "ListStrView")]
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
    ($structname:ident (module = $module:literal, name = $name:literal, item = $item:ty as $itemview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;

            #[pyclass(module = $module, name = $name)]
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
        }
    };
}

/// Generating a temporary map view of a Rust's map. This view allows random access at O(1) cost.
///
/// # Placeholder
///
/// * structname: name of the module that will contains the mapview
/// * module: name of the python module that will contains this struct
/// * name: name of the python class in the module
/// * derivetrait: optional, list of traits to derive for the struct
/// * itemview: the item view type (a pyo3 class) -- expect to have a constructor `new(&item)`
/// * item: the item type (rust struct)
///
/// # Example
///
/// # TODO: add example
#[macro_export]
macro_rules! pyset {
    ($structname:ident (module = $module:literal, name = $name:literal, item = $item:ty as $itemview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;

            #[pyclass(module = $module, name = $name)]
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

/// Generating a temporary map view of a Rust's map. This view allows random access at O(1) cost.
///
/// # Placeholder
///
/// * structname: name of the module that will contains the mapview
/// * module: name of the python module that will contains this struct
/// * name: name of the python class in the module
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
    ($structname:ident (module = $module:literal, name = $name:literal, key = $key:ty as $keyview:ty, value = $value:ty as $valueview:ty $(, derive = ($( $derivetrait:ident ),*) )? )) => {
        pub mod $structname {
            use kgdata::pyo3helper::unsafe_update_view_lifetime_signature;
            use pyo3::prelude::*;
            use pyo3::types::PyTuple;

            #[pyclass(module = $module, name = $name)]
            pub struct MapView(pub &'static hashbrown::HashMap<$key, $value>);
            #[pyclass]
            pub struct KeysView(pub hashbrown::hash_map::Keys<'static, $key, $value>);
            #[pyclass]
            pub struct ValuesView(pub hashbrown::hash_map::Values<'static, $key, $value>);
            #[pyclass]
            pub struct ItemsView(pub hashbrown::hash_map::Iter<'static, $key, $value>);

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

/// Generating a temporary view of a Rust's type, properties accessed in Python will trigger a conversion.
#[macro_export]
macro_rules! pyview {
    ($viewname:ident (module = $module:literal, name = $name:literal, cls = $clsname:ident $(, derive = ($( $derivetrait:ident ),*) )? ) $({
        $(
            $(c($cel:ident: $cty:ty))?  // c for copy
            $(b($bel:ident: $bty:ty))?  // b for borrow
            $(r($rel:ident: $rty:ty))?  // r for as_ref
            $(v($vel:ident: $vty:ty))?  // v for view -- return a temporary view which is assumed to have a constructor `new(&value)`
            $(f($func:ident: $returntype:ty))?  // f for function
            $(iter($itervec:ident { $iel:ident: $ity:ty }))?  // iter for iterator
        ),*
    })?) => {
        #[pyclass(module = $module, name = $name)]
        $(#[derive($($derivetrait,)*)])?
        pub struct $viewname(pub &'static $clsname);

        impl $viewname {
            pub fn new(value: &$clsname) -> Self {
                Self(unsafe_update_view_lifetime_signature(value))
            }
        }

        #[pymethods]
        impl $viewname {
            $($(
                $(
                    #[getter]
                    fn $cel(&self) -> $cty {
                        self.0.$cel
                    }
                )?

                $(
                    #[getter]
                    fn $bel(&self) -> &$bty {
                        &self.0.$bel
                    }
                )?

                $(
                    #[getter]
                    fn $rel(&self) -> $rty {
                        self.0.$rel.as_ref()
                    }
                )?

                $(
                    #[getter]
                    fn $vel(&self) -> $vty {
                        <$vty>::new(&self.0.$vel)
                    }
                )?

                $(
                    fn $func(&self) -> $returntype {
                        self.0.$func()
                    }
                )?

                $(
                    fn $itervec(&self) -> $ity {
                        <$ity>::new(&self.0.$iel)
                    }
                )?
            )*)?
        }
    };
}

/// Generating a Python wrapper of a Rust's type, properties accessed in Python will trigger a conversion. Different from view, this wrapper owns the data.
#[macro_export]
macro_rules! pywrap {
    ($wrapper:ident (module = $module:literal, name = $name:literal, cls = $clsname:ident $(, derive = ($( $derivetrait:ident ),*) )? ) $({
        $(
            $(c($cel:ident: $cty:ty))?  // c for copy
            $(b($bel:ident: $bty:ty))?  // b for borrow
            $(r($rel:ident: $rty:ty))?  // r for as_ref
            $(v($vel:ident: $vty:ty))?  // v for view -- return a temporary view, which is assumed to have a constructor `new(&value)`
            $(f($func:ident: $returntype:ty))?  // f for function
            $(iter($itervec:ident { $iel:ident: $ity:ty }))?  // iter for iterator
        ),*
    })?) => {
        #[pyclass(module = $module, name = $name)]
        $(#[derive($($derivetrait,)*)])?
        pub struct $wrapper(pub $clsname);

        impl $wrapper {
            pub fn new(value: $clsname) -> Self {
                Self(value)
            }
        }

        #[pymethods]
        impl $wrapper {
            $($(
                $(
                    #[getter]
                    fn $cel(&self) -> $cty {
                        self.0.$cel
                    }
                )?

                $(
                    #[getter]
                    fn $bel(&self) -> &$bty {
                        &self.0.$bel
                    }
                )?

                $(
                    #[getter]
                    fn $rel(&self) -> $rty {
                        self.0.$rel.as_ref()
                    }
                )?

                $(
                    #[getter]
                    fn $vel(&self) -> $vty {
                        <$vty>::new(&self.0.$vel)
                    }
                )?

                $(
                    fn $func(&self) -> $returntype {
                        self.0.$func()
                    }
                )?

                $(
                    fn $itervec(&self) -> $ity {
                        <$ity>::new(&self.0.$iel)
                    }
                )?
            )*)?
        }
    };
}

/// Generating a Python's twin of a Rust's type where each property of the Rust's type is now a Python object to avoid conversion overhead.
#[macro_export]
macro_rules! pymirror {
    ($pycls:ident (module = $module:literal, name = $name:literal, cls = $rustcls:ident $(, derive = ($( $derivetrait:ident ),*) )? ) {
        $(
            $( c($cel:ident) )? // c for copy
            $( b($bel:ident) )? // b for borrow
            $( r($oel:ident) )? // r for as_ref
        ),*
    }) => {
        #[pyclass(module = $module, name = $name)]
        $(#[derive($($derivetrait,)*)])?
        pub struct $pycls {
            $(
                #[pyo3(get,set)]
                $( $cel: PyObject, )?
                $( $bel: PyObject, )?
                $( $oel: PyObject, )?
            )*
        }

        impl $pycls {
            pub fn new(py: Python<'_>, value: &$rustcls) -> Self {
                Self {
                    $(
                        $(
                            $cel: value.$cel.into_py(py),
                        )?
                        $(
                            $bel: (&value.$bel).into_py(py),
                        )?
                        $(
                            $oel: value.$oel.as_ref().into_py(py),
                        )?
                    )*
                }
            }
        }
    };
}

/// Change signature of a reference from temporary to static. This is unsafe and
/// only be used for temporary views that drop immediately after use.
pub fn unsafe_update_view_lifetime_signature<T: ?Sized>(val: &T) -> &'static T {
    let ptr = val as *const T;
    unsafe { &*ptr }
}

pub fn unsafe_update_mut_view_lifetime_signature<T: ?Sized>(val: &mut T) -> &'static mut T {
    let ptr = val as *mut T;
    unsafe { &mut *ptr }
}
