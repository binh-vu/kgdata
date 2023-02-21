use crate::models::{EntityId, GlobeCoordinate, MonolingualText, Quantity, Time, Value};
use pyo3::prelude::*;

/// Python wrapper for Value struct.
#[derive(Clone, Debug)]
#[pyclass(module = "kgdata.core.models", name = "ValueView")]
pub struct PyValueView {
    pub value: &'static Value,
}

impl PyValueView {
    pub fn new(value: *const Value) -> Self {
        PyValueView {
            value: unsafe { &*value },
        }
    }
}

#[pymethods]
impl PyValueView {
    pub fn get_type(&self) -> &'static str {
        match &self.value {
            Value::String(_) => "string",
            Value::EntityId(_) => "entity-id",
            Value::Quantity(_) => "quantity",
            Value::Time(_) => "time",
            Value::GlobeCoordinate(_) => "globe-coordinate",
            Value::MonolingualText(_) => "monolingual-text",
        }
    }

    pub fn is_str(&self) -> bool {
        match &self.value {
            Value::String(_) => true,
            _ => false,
        }
    }

    pub fn is_entity_id(&self) -> bool {
        match &self.value {
            Value::EntityId(_) => true,
            _ => false,
        }
    }

    pub fn is_quantity(&self) -> bool {
        match &self.value {
            Value::Quantity(_) => true,
            _ => false,
        }
    }

    pub fn is_time(&self) -> bool {
        match &self.value {
            Value::Time(_) => true,
            _ => false,
        }
    }

    pub fn is_globe_coordinate(&self) -> bool {
        match &self.value {
            Value::GlobeCoordinate(_) => true,
            _ => false,
        }
    }

    pub fn is_monolingual_text(&self) -> bool {
        match &self.value {
            Value::MonolingualText(_) => true,
            _ => false,
        }
    }

    pub fn as_str(&self) -> PyResult<&String> {
        match &self.value {
            Value::String(s) => Ok(s),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a string",
            )),
        }
    }

    pub fn as_entity_id_str(&self) -> PyResult<&String> {
        match &self.value {
            Value::EntityId(s) => Ok(&s.id),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_entity_id(&self, py: Python) -> PyResult<PyEntityId> {
        match &self.value {
            Value::EntityId(s) => Ok(PyEntityId::new(py, s)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_quantity(&self, py: Python) -> PyResult<PyQuantity> {
        match &self.value {
            Value::Quantity(v) => Ok(PyQuantity::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a quantity",
            )),
        }
    }

    pub fn as_time(&self, py: Python) -> PyResult<PyTime> {
        match &self.value {
            Value::Time(v) => Ok(PyTime::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a time",
            )),
        }
    }

    pub fn as_globe_coordinate(&self, py: Python) -> PyResult<PyGlobeCoordinate> {
        match &self.value {
            Value::GlobeCoordinate(v) => Ok(PyGlobeCoordinate::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }

    pub fn as_monolingual_text(&self, py: Python) -> PyResult<PyMonolingualText> {
        match &self.value {
            Value::MonolingualText(v) => Ok(PyMonolingualText::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }

    pub fn to_string_repr(&self) -> String {
        serde_json::to_string(&self.value).unwrap()
    }
}

#[pyclass(module = "kgdata.core.models", name = "EntityId")]
pub struct PyEntityId {
    #[pyo3(get, set)]
    id: PyObject,
    #[pyo3(get, set)]
    entity_type: PyObject,
    #[pyo3(get, set)]
    numeric_id: PyObject,
}

impl PyEntityId {
    pub fn new(py: Python<'_>, value: &EntityId) -> Self {
        PyEntityId {
            id: (&value.id).into_py(py),
            entity_type: value.entity_type.to_str().into_py(py),
            numeric_id: value.numeric_id.into_py(py),
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "Time")]
pub struct PyTime {
    #[pyo3(get, set)]
    time: PyObject,
    #[pyo3(get, set)]
    timezone: PyObject,
    #[pyo3(get, set)]
    before: PyObject,
    #[pyo3(get, set)]
    after: PyObject,
    #[pyo3(get, set)]
    precision: PyObject,
    #[pyo3(get, set)]
    calendarmodel: PyObject,
}

impl PyTime {
    pub fn new(py: Python<'_>, value: &Time) -> Self {
        PyTime {
            time: (&value.time).into_py(py),
            timezone: value.timezone.into_py(py),
            before: value.before.into_py(py),
            after: value.after.into_py(py),
            precision: value.precision.into_py(py),
            calendarmodel: (&value.calendarmodel).into_py(py),
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "Quantity")]
pub struct PyQuantity {
    #[pyo3(get, set)]
    amount: PyObject,
    #[pyo3(get, set)]
    upper_bound: PyObject,
    #[pyo3(get, set)]
    lower_bound: PyObject,
    #[pyo3(get, set)]
    unit: PyObject,
}

impl PyQuantity {
    pub fn new(py: Python<'_>, value: &Quantity) -> Self {
        PyQuantity {
            amount: (&value.amount).into_py(py),
            upper_bound: value.upper_bound.as_ref().into_py(py),
            lower_bound: value.lower_bound.as_ref().into_py(py),
            unit: (&value.unit).into_py(py),
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "GlobeCoordinate")]
pub struct PyGlobeCoordinate {
    #[pyo3(get, set)]
    latitude: PyObject,
    #[pyo3(get, set)]
    longitude: PyObject,
    #[pyo3(get, set)]
    precision: PyObject,
    #[pyo3(get, set)]
    altitude: PyObject,
    #[pyo3(get, set)]
    globe: PyObject,
}

impl PyGlobeCoordinate {
    pub fn new(py: Python<'_>, value: &GlobeCoordinate) -> Self {
        PyGlobeCoordinate {
            latitude: value.latitude.into_py(py),
            longitude: value.longitude.into_py(py),
            precision: value.precision.into_py(py),
            altitude: value.altitude.into_py(py),
            globe: (&value.globe).into_py(py),
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "MonolingualText")]
pub struct PyMonolingualText {
    #[pyo3(get, set)]
    text: PyObject,
    #[pyo3(get, set)]
    language: PyObject,
}

impl PyMonolingualText {
    pub fn new(py: Python<'_>, value: &MonolingualText) -> Self {
        PyMonolingualText {
            text: (&value.text).into_py(py),
            language: (&value.language).into_py(py),
        }
    }
}
