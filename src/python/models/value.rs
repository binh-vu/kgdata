use crate::error::into_pyerr;
use crate::models::{
    EntityId, EntityType, GlobeCoordinate, MonolingualText, Quantity, Time, Value,
};
use crate::pyo3helper::unsafe_update_view_lifetime_signature;
use crate::{pylist, pymirror, pyview, pywrap};
use pyo3::prelude::*;

pyview!(
    ValueView(module = "kgdata.core.models", name = "ValueView", cls = Value, derive=(Clone, Debug)) {
        f(get_type: &'static str),
        f(is_str: bool),
        f(is_entity_id: bool),
        f(is_quantity: bool),
        f(is_time: bool),
        f(is_globe_coordinate: bool),
        f(is_monolingual_text: bool),
        f(to_string_repr: String),
    }
);
pywrap!(
    PyValue(module = "kgdata.core.models", name = "Value", cls = Value, derive=(Clone, Debug)) {
        f(get_type: &'static str),
        f(is_str: bool),
        f(is_entity_id: bool),
        f(is_quantity: bool),
        f(is_time: bool),
        f(is_globe_coordinate: bool),
        f(is_monolingual_text: bool),
        f(to_string_repr: String)
    }
);
pylist!(value_list_view(
    module = "kgdata.core.models",
    item = super::Value as super::ValueView
));

#[pymethods]
impl ValueView {
    pub fn as_str(&self) -> PyResult<&String> {
        match &self.0 {
            Value::String(s) => Ok(s),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a string",
            )),
        }
    }

    pub fn as_entity_id_str(&self) -> PyResult<&String> {
        match &self.0 {
            Value::EntityId(s) => Ok(&s.id),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_entity_id(&self, py: Python) -> PyResult<PyEntityId> {
        match &self.0 {
            Value::EntityId(s) => Ok(PyEntityId::new(py, s)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_quantity(&self, py: Python) -> PyResult<PyQuantity> {
        match &self.0 {
            Value::Quantity(v) => Ok(PyQuantity::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a quantity",
            )),
        }
    }

    pub fn as_time(&self, py: Python) -> PyResult<PyTime> {
        match &self.0 {
            Value::Time(v) => Ok(PyTime::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a time",
            )),
        }
    }

    pub fn as_globe_coordinate(&self, py: Python) -> PyResult<PyGlobeCoordinate> {
        match &self.0 {
            Value::GlobeCoordinate(v) => Ok(PyGlobeCoordinate::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }

    pub fn as_monolingual_text(&self, py: Python) -> PyResult<PyMonolingualText> {
        match &self.0 {
            Value::MonolingualText(v) => Ok(PyMonolingualText::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }
}

#[pymethods]
impl PyValue {
    #[staticmethod]
    pub fn string(s: String) -> PyValue {
        PyValue(Value::String(s))
    }

    #[staticmethod]
    pub fn entity_id(id: String, entity_type: &str, numeric_id: Option<u64>) -> PyResult<PyValue> {
        Ok(PyValue(Value::EntityId(EntityId {
            id,
            entity_type: EntityType::from_str(&entity_type).map_err(into_pyerr)?,
            numeric_id,
        })))
    }

    #[staticmethod]
    pub fn time(
        time: String,
        timezone: u64,
        before: u64,
        after: u64,
        precision: u64,
        calendarmodel: String,
    ) -> PyValue {
        PyValue(Value::Time(Time {
            time,
            timezone,
            before,
            after,
            precision,
            calendarmodel,
        }))
    }

    #[staticmethod]
    #[pyo3(signature = (amount, lower_bound, upper_bound, unit))]
    pub fn quantity(
        amount: String,
        lower_bound: Option<String>,
        upper_bound: Option<String>,
        unit: String,
    ) -> PyValue {
        PyValue(Value::Quantity(Quantity {
            amount,
            lower_bound,
            upper_bound,
            unit,
        }))
    }

    #[staticmethod]
    #[pyo3(signature = (latitude, longitude, precision, altitude, globe))]
    pub fn globe_coordinate(
        latitude: f64,
        longitude: f64,
        precision: Option<f64>,
        altitude: Option<f64>,
        globe: String,
    ) -> PyValue {
        PyValue(Value::GlobeCoordinate(GlobeCoordinate {
            latitude,
            longitude,
            altitude,
            precision,
            globe,
        }))
    }

    #[staticmethod]
    pub fn monolingual_text(text: String, language: String) -> PyValue {
        PyValue(Value::MonolingualText(MonolingualText { text, language }))
    }

    pub fn as_str(&self) -> PyResult<&String> {
        match &self.0 {
            Value::String(s) => Ok(s),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a string",
            )),
        }
    }

    pub fn as_entity_id_str(&self) -> PyResult<&String> {
        match &self.0 {
            Value::EntityId(s) => Ok(&s.id),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_entity_id(&self, py: Python) -> PyResult<PyEntityId> {
        match &self.0 {
            Value::EntityId(s) => Ok(PyEntityId::new(py, s)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not an entity id",
            )),
        }
    }

    pub fn as_quantity(&self, py: Python) -> PyResult<PyQuantity> {
        match &self.0 {
            Value::Quantity(v) => Ok(PyQuantity::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a quantity",
            )),
        }
    }

    pub fn as_time(&self, py: Python) -> PyResult<PyTime> {
        match &self.0 {
            Value::Time(v) => Ok(PyTime::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a time",
            )),
        }
    }

    pub fn as_globe_coordinate(&self, py: Python) -> PyResult<PyGlobeCoordinate> {
        match &self.0 {
            Value::GlobeCoordinate(v) => Ok(PyGlobeCoordinate::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }

    pub fn as_monolingual_text(&self, py: Python) -> PyResult<PyMonolingualText> {
        match &self.0 {
            Value::MonolingualText(v) => Ok(PyMonolingualText::new(py, v)),
            _ => Err(pyo3::exceptions::PyTypeError::new_err(
                "Value is not a globe coordinate",
            )),
        }
    }
}

// =================================================================================================
// Now to the list of specific value types that are located in Python's heap to avoid repeated conversion overhead.

pymirror!(PyEntityId(module = "kgdata.core.models", name = "EntityId", cls = EntityId) {
    b(id), b(entity_type), c(numeric_id)
});
pymirror!(PyTime(module = "kgdata.core.models", name = "Time", cls = Time) {
    b(time), c(timezone), c(before), c(after), c(precision), b(calendarmodel)
});
pymirror!(PyQuantity(module = "kgdata.core.models", name = "Quantity", cls = Quantity) {
    b(amount), r(upper_bound), r(lower_bound), b(unit)
});
pymirror!(PyGlobeCoordinate(module = "kgdata.core.models", name = "GlobeCoordinate", cls = GlobeCoordinate) {
    c(latitude), c(longitude), c(precision), c(altitude), b(globe)
});
pymirror!(PyMonolingualText(module = "kgdata.core.models", name = "MonolingualText", cls = MonolingualText) {
    b(text), b(language)
});
