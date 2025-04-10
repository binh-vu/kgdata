pub mod list;
pub mod map;
pub mod set;

pub use self::list::*;
pub use self::map::*;
pub use self::set::*;

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
#[inline]
pub fn unsafe_update_view_lifetime_signature<T: ?Sized>(val: &T) -> &'static T {
    return unsafe { std::mem::transmute(val) };
}

#[inline]
pub fn unsafe_update_mut_view_lifetime_signature<T: ?Sized>(val: &mut T) -> &'static mut T {
    let ptr = val as *mut T;
    unsafe { &mut *ptr }
}
