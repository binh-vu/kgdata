use crate::models::{Statement, Value};

use super::super::entity::Entity;
use super::entity::PyStatementView;
use super::value::PyValueView;
use crate::macros::unsafe_update_view_lifetime_signature;

use hashbrown::hash_map;
use pyo3::prelude::*;

#[pyclass(module = "kgdata.core.models", name = "EntityPropsKeysView")]
pub struct PyEntityPropsKeysView {
    iter: hash_map::Keys<'static, String, Vec<Statement>>,
}

impl PyEntityPropsKeysView {
    pub fn new(container: &Entity) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&container.props).keys();
        PyEntityPropsKeysView { iter }
    }
}

#[pymethods]
impl PyEntityPropsKeysView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "EntityPropsValuesView")]
pub struct PyEntityPropsValuesView {
    iter: hash_map::Values<'static, String, Vec<Statement>>,
}

impl PyEntityPropsValuesView {
    pub fn new(container: &Entity) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&container.props).values();
        PyEntityPropsValuesView { iter }
    }
}

#[pymethods]
impl PyEntityPropsValuesView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyStatementsView> {
        if let Some(v) = self.iter.next() {
            Some(PyStatementsView::new(v))
        } else {
            None
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "EntityPropsItemsView")]
pub struct PyEntityPropsItemsView {
    iter: hash_map::Iter<'static, String, Vec<Statement>>,
}

impl PyEntityPropsItemsView {
    pub fn new(container: &Entity) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&container.props).iter();
        PyEntityPropsItemsView { iter }
    }
}

#[pymethods]
impl PyEntityPropsItemsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<(&String, PyStatementsView)> {
        if let Some((k, v)) = self.iter.next() {
            Some((k, PyStatementsView::new(v)))
        } else {
            None
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "StatementsView")]
pub struct PyStatementsView {
    lst: &'static Vec<Statement>,
    iter: std::slice::Iter<'static, Statement>,
}

impl PyStatementsView {
    pub fn new(lst: &Vec<Statement>) -> Self {
        let lst = unsafe_update_view_lifetime_signature(lst);
        let iter = lst.iter();

        PyStatementsView { iter, lst }
    }
}

#[pymethods]
impl PyStatementsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyStatementView> {
        if let Some(s) = self.iter.next() {
            Some(PyStatementView::new(s))
        } else {
            None
        }
    }

    fn __getitem__(&self, i: usize) -> PyResult<PyStatementView> {
        if i < self.lst.len() {
            Ok(PyStatementView::new(&self.lst[i]))
        } else {
            Err(pyo3::exceptions::PyIndexError::new_err(
                "index out of range",
            ))
        }
    }

    fn __len__(&self) -> usize {
        self.lst.len()
    }
}

#[pyclass(module = "kgdata.core.models", name = "QualifiersKeysView")]
pub struct PyQualifiersKeysView {
    iter: hash_map::Keys<'static, String, Vec<Value>>,
}

impl PyQualifiersKeysView {
    pub fn new(statement: &Statement) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&statement.qualifiers).keys();
        PyQualifiersKeysView { iter }
    }
}

#[pymethods]
impl PyQualifiersKeysView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "QualifiersValuesView")]
pub struct PyQualifiersValuesView {
    iter: hash_map::Values<'static, String, Vec<Value>>,
}

impl PyQualifiersValuesView {
    pub fn new(statement: &Statement) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&statement.qualifiers).values();
        PyQualifiersValuesView { iter }
    }
}

#[pymethods]
impl PyQualifiersValuesView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyValuesView> {
        if let Some(v) = self.iter.next() {
            Some(PyValuesView::new(v))
        } else {
            None
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "QualifiersItemsView")]
pub struct PyQualifiersItemsView {
    iter: hash_map::Iter<'static, String, Vec<Value>>,
}

impl PyQualifiersItemsView {
    pub fn new(statement: &Statement) -> Self {
        let iter = unsafe_update_view_lifetime_signature(&statement.qualifiers).iter();
        PyQualifiersItemsView { iter }
    }
}

#[pymethods]
impl PyQualifiersItemsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<(&String, PyValuesView)> {
        if let Some((k, v)) = self.iter.next() {
            Some((k, PyValuesView::new(v)))
        } else {
            None
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "ValuesView")]
pub struct PyValuesView {
    lst: &'static Vec<Value>,
    iter: std::slice::Iter<'static, Value>,
}

impl PyValuesView {
    pub fn new(lst: &Vec<Value>) -> Self {
        let lst = unsafe_update_view_lifetime_signature(lst);
        let iter = lst.iter();
        PyValuesView { lst, iter }
    }
}

#[pymethods]
impl PyValuesView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyValueView> {
        if let Some(s) = self.iter.next() {
            Some(PyValueView(s))
        } else {
            None
        }
    }

    fn __getitem__(&self, i: usize) -> PyResult<PyValueView> {
        if i < self.lst.len() {
            Ok(PyValueView(&self.lst[i]))
        } else {
            Err(pyo3::exceptions::PyIndexError::new_err(
                "index out of range",
            ))
        }
    }

    fn __len__(&self) -> usize {
        self.lst.len()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringsView")]
pub struct PyStringsView {
    lst: &'static Vec<String>,
    iter: std::slice::Iter<'static, String>,
}

impl PyStringsView {
    pub fn new(lst: &Vec<String>) -> Self {
        let lst = unsafe_update_view_lifetime_signature(lst);
        let iter = lst.iter();
        PyStringsView { lst, iter }
    }
}

#[pymethods]
impl PyStringsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        if let Some(s) = self.iter.next() {
            Some(s)
        } else {
            None
        }
    }

    fn __getitem__(&self, i: usize) -> PyResult<&String> {
        if i < self.lst.len() {
            Ok(&self.lst[i])
        } else {
            Err(pyo3::exceptions::PyIndexError::new_err(
                "index out of range",
            ))
        }
    }

    fn __len__(&self) -> usize {
        self.lst.len()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringKeysView")]
pub struct PyStringKeysView {
    iter: hash_map::Keys<'static, String, String>,
}

impl PyStringKeysView {
    pub fn new(map: &hashbrown::HashMap<String, String>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).keys();
        PyStringKeysView { iter }
    }
}

#[pymethods]
impl PyStringKeysView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringValuesView")]
pub struct PyStringValuesView {
    iter: hash_map::Values<'static, String, String>,
}

impl PyStringValuesView {
    pub fn new(map: &hashbrown::HashMap<String, String>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).values();
        PyStringValuesView { iter }
    }
}

#[pymethods]
impl PyStringValuesView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringItemsView")]
pub struct PyStringItemsView {
    iter: hash_map::Iter<'static, String, String>,
}

impl PyStringItemsView {
    pub fn new(map: &hashbrown::HashMap<String, String>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).iter();
        PyStringItemsView { iter }
    }
}

#[pymethods]
impl PyStringItemsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<(&String, &String)> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringsKeysView")]
pub struct PyStringsKeysView {
    iter: hash_map::Keys<'static, String, Vec<String>>,
}

impl PyStringsKeysView {
    pub fn new(map: &hashbrown::HashMap<String, Vec<String>>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).keys();
        PyStringsKeysView { iter }
    }
}

#[pymethods]
impl PyStringsKeysView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<&String> {
        self.iter.next()
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringsValuesView")]
pub struct PyStringsValuesView {
    iter: hash_map::Values<'static, String, Vec<String>>,
}

impl PyStringsValuesView {
    pub fn new(map: &hashbrown::HashMap<String, Vec<String>>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).values();
        PyStringsValuesView { iter }
    }
}

#[pymethods]
impl PyStringsValuesView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyStringsView> {
        if let Some(v) = self.iter.next() {
            Some(PyStringsView::new(v))
        } else {
            None
        }
    }
}

#[pyclass(module = "kgdata.core.models", name = "StringsItemsView")]
pub struct PyStringsItemsView {
    iter: hash_map::Iter<'static, String, Vec<String>>,
}

impl PyStringsItemsView {
    pub fn new(map: &hashbrown::HashMap<String, Vec<String>>) -> Self {
        let iter = unsafe_update_view_lifetime_signature(map).iter();
        PyStringsItemsView { iter }
    }
}

#[pymethods]
impl PyStringsItemsView {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<(&String, PyStringsView)> {
        if let Some((k, v)) = self.iter.next() {
            Some((k, PyStringsView::new(v)))
        } else {
            None
        }
    }
}
