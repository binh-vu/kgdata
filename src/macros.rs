#[macro_export]
macro_rules! pyiter {
    ($clsname:ident (module = $module:literal, name = $name:literal) { $itemview:ident: $item:ident }) => {
        #[pyclass(module = $module, name = $name)]
        pub struct $clsname {
            lst: &'static [$item],
            iter: std::slice::Iter<'static, $item>,
        }

        impl $clsname {
            fn new(lst: &[$item]) -> Self {
                let lst2 = unsafe_update_view_lifetime_signature(lst);
                Self {
                    lst: lst2,
                    iter: lst2.iter(),
                }
            }
        }

        #[pymethods]
        impl $clsname {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(&mut self) -> Option<$itemview> {
                if let Some(v) = self.iter.next() {
                    Some($itemview(unsafe_update_view_lifetime_signature(v)))
                } else {
                    None
                }
            }

            fn __len__(&self) -> usize {
                self.lst.len()
            }

            fn __getitem__(&self, i: usize) -> PyResult<$itemview> {
                if i < self.lst.len() {
                    Ok($itemview(unsafe_update_view_lifetime_signature(
                        &self.lst[i],
                    )))
                } else {
                    Err(pyo3::exceptions::PyIndexError::new_err(
                        "index out of range",
                    ))
                }
            }
        }
    };
}

/// Generating a temporary view of a Rust's type.
#[macro_export]
macro_rules! pyview {
    ($viewname:ident (module = $module:literal, name = $name:literal, cls = $clsname:ident $(, derive = ($( $derivetrait:ident ),*) )? ) $({
        $(
            $(c($cel:ident: $cty:ty))?  // c for copy
            $(r($rel:ident: $rty:ty))?  // r for reference
            $(f($func:ident: $returntype:ty))?  // f for function
            $(iter($itervec:ident { $iel:ident: $ity:ty }))?  // iter for iterator
        ),*
    })?) => {
        #[pyclass(module = $module, name = $name)]
        $(#[derive($($derivetrait,)*)])?
        pub struct $viewname(pub &'static $clsname);

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
                    fn $rel(&self) -> &$rty {
                        &self.0.$rel
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

/// Generating a Python wrapper of a Rust's type, properties accessed in Python will trigger a conversion.
#[macro_export]
macro_rules! pywrap {
    ($wrapper:ident (module = $module:literal, name = $name:literal, cls = $clsname:ident $(, derive = ($( $derivetrait:ident ),*) )? ) $({
        $(
            $(c($cel:ident: $cty:ty))?  // c for copy
            $(r($rel:ident: $rty:ty))?  // r for reference
            $(f($func:ident: $returntype:ty))?  // f for function
            $(iter($itervec:ident { $iel:ident: $ity:ty }))?  // iter for iterator
        ),*
    })?) => {
        #[pyclass(module = $module, name = $name)]
        $(#[derive($($derivetrait,)*)])?
        pub struct $wrapper(pub $clsname);

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
                    fn $rel(&self) -> &$rty {
                        &self.0.$rel
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
