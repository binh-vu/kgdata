use super::super::entity::Entity;
use super::iterators::{
    PyEntityPropsItemsView, PyEntityPropsKeysView, PyEntityPropsValuesView, PyQualifiersItemsView,
    PyQualifiersKeysView, PyQualifiersValuesView, PyStatementsView, PyStringItemsView,
    PyStringKeysView, PyStringValuesView, PyStringsItemsView, PyStringsKeysView,
    PyStringsValuesView, PyStringsView, PyValuesView,
};
use super::value::PyValueView;
use crate::conversions::WDEntity;
use crate::error::into_pyerr;
use crate::models::Statement;
use pyo3::prelude::*;
use pyo3::types::PyString;
use std::sync::Arc;

/// A Python wrapper for the Entity struct.
///
/// Note that to make this struct efficient, anything returns by a function that is not a primitive (i.e., instance of a class ending with View) contains a pointer to the Entity struct in Rust
/// and should not be stored in Python without converting it to a Python object because the data can be moved to new address
/// (e.g., resize vector, hashmap) rendering the pointer invalid.
///
/// On the other hand, property getters are safe to use and store because they return a copy of the data.
#[pyclass(name = "Entity")]
pub struct PyEntity {
    pub entity: Arc<Entity>,
    pub label: Option<Py<PyString>>,
}

#[pymethods]
impl PyEntity {
    /// Create a new Entity from a JSON string.
    #[staticmethod]
    fn from_wdentity_json(data: &[u8]) -> PyResult<Self> {
        let ent = serde_json::from_slice::<WDEntity>(data)
            .map_err(into_pyerr)?
            .0;
        return Ok(PyEntity {
            entity: Arc::new(ent),
            label: None,
        });
    }

    /// Get the entity ID.
    #[getter]
    fn id(&self) -> &str {
        &self.entity.id
    }

    /// Get the entity type.
    fn entity_type(&self) -> &'static str {
        self.entity.entity_type.to_str()
    }

    /// Get the entity label in default language.
    #[getter]
    fn label(&mut self, py: Python<'_>) -> &Py<PyString> {
        if self.label.is_none() {
            self.label = Some(
                PyString::new(py, &self.entity.label.lang2value[&self.entity.label.lang]).into(),
            );
        }
        self.label.as_ref().unwrap()
    }

    /// Get the default language of the entity label.
    fn label_default_lang(&self) -> &String {
        &self.entity.label.lang
    }

    /// Get the entity label in a specific language.
    fn label_in_lang(&self, lang: &str) -> &String {
        &self.entity.label.lang2value[lang]
    }

    /// Get list of languages that the entity label is available in.
    fn label_keys(&self) -> PyStringKeysView {
        PyStringKeysView::new(&self.entity.label.lang2value)
    }

    /// Get list of labels in all languages.
    fn label_values(&self) -> PyStringValuesView {
        PyStringValuesView::new(&self.entity.label.lang2value)
    }

    /// Get list of labels and the corresponding languages.
    fn label_items(&self) -> PyStringItemsView {
        PyStringItemsView::new(&self.entity.label.lang2value)
    }

    /// Get the entity description in default language.
    fn description(&self) -> &String {
        &self.entity.description.lang2value[&self.entity.description.lang]
    }

    /// Get the default language of the entity description.
    fn description_default_lang(&self) -> &String {
        &self.entity.description.lang
    }

    /// Get the entity description in a specific language.
    fn description_in_lang(&self, lang: &str) -> &String {
        &self.entity.description.lang2value[lang]
    }

    /// Get list of languages that the entity description is available in.
    fn description_keys(&self) -> PyStringKeysView {
        PyStringKeysView::new(&self.entity.description.lang2value)
    }

    /// Get list of descriptions in all languages.
    fn description_values(&self) -> PyStringValuesView {
        PyStringValuesView::new(&self.entity.description.lang2value)
    }

    /// Get list of descriptions and the corresponding languages.
    fn description_items(&self) -> PyStringItemsView {
        PyStringItemsView::new(&self.entity.description.lang2value)
    }

    /// Get the entity aliases in default language.
    fn aliases(&self) -> PyStringsView {
        PyStringsView::new(&self.entity.aliases.lang2values[&self.entity.aliases.lang])
    }

    /// Get the default language of the entity aliases.
    fn aliases_default_lang(&self) -> &String {
        &self.entity.aliases.lang
    }

    /// Get the entity aliases in a specific language.
    fn aliases_in_lang(&self, lang: &str) -> PyStringsView {
        PyStringsView::new(&self.entity.aliases.lang2values[lang])
    }

    /// Get list of languages that the entity aliases is available in.
    fn aliases_keys(&self) -> PyStringsKeysView {
        PyStringsKeysView::new(&self.entity.aliases.lang2values)
    }

    /// Get list of aliasess in all languages.
    fn aliases_values(&self) -> PyStringsValuesView {
        PyStringsValuesView::new(&self.entity.aliases.lang2values)
    }

    /// Get list of aliasess and the corresponding languages.
    fn aliases_items(&self) -> PyStringsItemsView {
        PyStringsItemsView::new(&self.entity.aliases.lang2values)
    }

    /// Iterate over each property of the entity.
    fn props_keys(&self) -> PyEntityPropsKeysView {
        PyEntityPropsKeysView::new(&self.entity)
    }

    /// Iterate over each property's value of the entity.
    fn props_values(&self) -> PyEntityPropsValuesView {
        PyEntityPropsValuesView::new(&self.entity)
    }

    /// Iterate over each property and its value of the entity.
    fn props_items(&self) -> PyEntityPropsItemsView {
        PyEntityPropsItemsView::new(&self.entity)
    }

    /// Get the statements of a property.
    fn prop(&self, id: &str) -> PyStatementsView {
        PyStatementsView::new(&self.entity.props[id])
    }
}

#[pyclass(name = "StatementView")]
pub struct PyStatementView {
    pub statement: &'static Statement,
}

impl PyStatementView {
    pub fn new(statement: *const Statement) -> Self {
        PyStatementView {
            statement: unsafe { &*statement },
        }
    }
}

#[pymethods]
impl PyStatementView {
    fn value(&self) -> PyValueView {
        PyValueView::new(&self.statement.value)
    }

    fn qualifiers_keys(&self) -> PyQualifiersKeysView {
        PyQualifiersKeysView::new(self.statement)
    }

    fn qualifiers_values(&self) -> PyQualifiersValuesView {
        PyQualifiersValuesView::new(self.statement)
    }

    fn qualifiers_items(&self) -> PyQualifiersItemsView {
        PyQualifiersItemsView::new(self.statement)
    }

    fn qualifiers_order(&self) -> PyStringsView {
        PyStringsView::new(&self.statement.qualifiers_order)
    }

    fn qualifier(&self, id: &str) -> PyValuesView {
        PyValuesView::new(&self.statement.qualifiers[id])
    }

    fn rank(&self) -> &'static str {
        self.statement.rank.to_str()
    }
}
