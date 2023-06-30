use std::marker::PhantomData;
use std::path::Path;
use std::str;

use crate::{container::Container, macros::def_pyfunction};
use std::fmt;

use postcard::{from_bytes, to_allocvec, to_vec};
use pyo3::conversion::IntoPy;
use pyo3::type_object::PyTypeObject;
use pyo3::types::{PyBytes, PyDict, PyString};
use pyo3::{prelude::*, types::PyTuple};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};


#[derive(Serialize, Deserialize)]
struct Object<'s> {
    id: &'s str,
    type_: &'s str,
    datatype: Option<&'s str>,
    label: MultiLingualString<'s>,
    description: MultiLingualString<'s>,
    props: WrappedPyDict<PyString, Vec<WDStatement<'s>>>,
}

#[derive(Serialize, Deserialize)]
// struct WDStatement<'s> { dict }
#[derive(Serialize, Deserialize)]
struct MultiLingualString<'s> {
    lang2value: WrappedPyDict<PyString, PyString>,
    lang: &'s str,
}

trait PySer {
    fn pyserialize(&self) -> PyResult<Vec<u8>>;
    fn pydeserialize(data: &[u8]) -> PyResult<&Self>
    where
        Self: Sized;
}

impl PySer for PyString {
    fn pyserialize(&self) -> PyResult<Vec<u8>> {
        return Ok(self.to_str().unwrap().as_bytes().to_owned());
    }
    fn pydeserialize(data: &[u8]) -> PyResult<&Self>
    where
        Self: Sized,
    {
        let py = unsafe { Python::assume_gil_acquired() };
        Ok(PyString::new(py, str::from_utf8(data).unwrap()))
    }
}

#[repr(transparent)]
struct WrappedPyDict<K, V> {
    dict: Py<PyDict>,
    kmarker: PhantomData<K>,
    vmarker: PhantomData<V>,
}

impl<K, V> WrappedPyDict<K, V> {
    fn new(dict: Py<PyDict>) -> Self {
        Self {
            dict,
            kmarker: PhantomData,
            vmarker: PhantomData,
        }
    }
}

impl<K, V> Serialize for WrappedPyDict<K, V>
where
    for<'py> K: PySer + PyTryFrom<'py>,
    for<'py> V: PySer + PyTryFrom<'py>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let py = unsafe { Python::assume_gil_acquired() };
        let obj = self.dict.as_ref(py);

        let mut map = serializer.serialize_map(Some(obj.len())).unwrap();

        for (key, value) in obj.iter() {
            map.serialize_entry(
                &key.downcast::<K>().unwrap().pyserialize().unwrap(),
                &value.downcast::<V>().unwrap().pyserialize().unwrap(),
            )
            .unwrap()
        }
        map.end()
    }
}

impl<'de, K, V> Deserialize<'de> for WrappedPyDict<K, V>
where
    for<'py> K: PySer + PyTryFrom<'py> + ToPyObject,
    for<'py> V: PySer + PyTryFrom<'py> + ToPyObject,
{
    fn deserialize<D>(deserializer: D) -> Result<WrappedPyDict<K, V>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(WrappedPyDictVisitor {
            kmarker: PhantomData,
            vmarker: PhantomData,
        })
    }
}

struct WrappedPyDictVisitor<K, V> {
    kmarker: PhantomData<K>,
    vmarker: PhantomData<V>,
}

impl<'de, K, V> Visitor<'de> for WrappedPyDictVisitor<K, V>
where
    for<'py> K: PySer + PyTryFrom<'py> + ToPyObject,
    for<'py> V: PySer + PyTryFrom<'py> + ToPyObject,
{
    type Value = WrappedPyDict<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a dict")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let py = unsafe { Python::assume_gil_acquired() };
        let mut dict = PyDict::new(py);
        while let Some((key, value)) = map.next_entry::<Vec<u8>, Vec<u8>>().unwrap() {
            dict.set_item(
                K::pydeserialize(&key).unwrap(),
                V::pydeserialize(&value).unwrap(),
            )
            .unwrap();
        }

        Ok(WrappedPyDict {
            dict: dict.into_py(py),
            kmarker: PhantomData,
            vmarker: PhantomData,
        })
    }
}

impl<'s> MultiLingualString<'s> {
    pub fn extract<'t>(py: Python<'_>, object: &'t PyAny) -> PyResult<MultiLingualString<'t>> {
        let lang_1 = object.getattr("lang")?;
        let lang = lang_1.cast_as::<PyString>()?.to_str()?;

        let lang2value_1 = object.getattr("lang2value")?;
        let aaa = lang2value_1.cast_as::<PyDict>()?;
        let lang2value = aaa.into_py(py);

        Ok(MultiLingualString {
            lang2value: WrappedPyDict::new(lang2value),
            lang,
        })
    }
}

#[pyfunction]
pub fn serent(py: Python<'_>, object: &PyAny) -> PyResult<Py<PyBytes>> {
    let id = object.getattr("id")?.downcast::<PyString>()?.to_str()?;
    let type_ = object.getattr("type")?.cast_as::<PyString>()?.to_str()?;

    let datatype_1 = object.getattr("datatype")?;
    let datatype = if datatype_1.is_none() {
        None
    } else {
        Some(datatype_1.cast_as::<PyString>()?.to_str()?)
    };

    let label = MultiLingualString::extract(py, object.getattr("label")?)?;
    let description = MultiLingualString::extract(py, object.getattr("description")?)?;

    let obj = Object {
        id,
        type_,
        datatype,
        label,
        description,
    };

    // let serdat = bincode::serialize(&obj).unwrap();
    let serdat = to_allocvec(&obj).unwrap();
    Ok(PyBytes::new(py, &serdat).into_py(py))
}

#[pyfunction]
pub fn deserent(py: Python<'_>, dat: &PyBytes) -> PyResult<Py<PyTuple>> {
    // let raw = bincode::deserialize::<Object>(dat.as_bytes()).unwrap();
    let raw = from_bytes::<Object>(dat.as_bytes()).unwrap();
    Ok(PyTuple::new(
        py,
        &[
            raw.id.to_object(py),
            raw.type_.to_object(py),
            raw.datatype.to_object(py),
            raw.label.lang2value.dict.to_object(py),
            raw.label.lang.to_object(py),
            raw.description.lang2value.dict.to_object(py),
            raw.description.lang.to_object(py),
        ],
    )
    .into_py(py))
}


pub(crate) fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let submodule = PyModule::new(py, "rocksdb")?;

    def_pyfunction!(submodule, "hugedict.hugedict.rocksdb", load);
    def_pyfunction!(submodule, "hugedict.hugedict.rocksdb", primary_db);
    def_pyfunction!(submodule, "hugedict.hugedict.rocksdb", stop_primary_db);
    def_pyfunction!(submodule, "hugedict.hugedict.rocksdb", serent);
    def_pyfunction!(submodule, "hugedict.hugedict.rocksdb", deserent);

    submodule.add_class::<Options>()?;
    submodule.add_class::<CompressionOptions>()?;
    submodule.add_class::<RocksDBDict>()?;
    submodule.add_class::<SecondaryDB>()?;
    submodule.add("PrefixExtractor", Container::type_object(py))?;
    submodule.add("fixed_prefix", Container::type_object(py))?;
    submodule.add("fixed_prefix_alike", Container::type_object(py))?;

    m.add_submodule(submodule)?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("hugedict.hugedict.rocksdb", submodule)?;

    Ok(())
}
