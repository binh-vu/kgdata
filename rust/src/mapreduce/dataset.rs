use core::hash::Hash;
use hashbrown::HashMap;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::path::PathBuf;

use crate::error::KGDataError;

use super::{FromParallelDataset, ParallelDataset};

pub struct Dataset<I> {
    pub items: Vec<I>,
}

pub struct MapDataset<K, V>
where
    K: Hash + Eq + Send,
    V: Send,
{
    pub map: HashMap<K, V>,
}

pub struct RefDataset<'t, I> {
    pub items: &'t Vec<I>,
}

impl<I> ParallelDataset for Dataset<I> where I: Send {}

impl<'t, I> ParallelDataset for RefDataset<'t, I> where I: Sync + 't {}

impl<'t, I> RefDataset<'t, I>
where
    I: 't,
{
    pub fn new(items: &'t Vec<I>) -> Self {
        Self { items }
    }
}

impl<K, V> ParallelDataset for MapDataset<K, V>
where
    K: Hash + Eq + Send,
    V: Send,
{
}

impl<I> IntoParallelIterator for Dataset<I>
where
    I: Send,
{
    type Iter = rayon::vec::IntoIter<Self::Item>;
    type Item = I;

    fn into_par_iter(self) -> Self::Iter {
        self.items.into_par_iter()
    }
}

impl<'t, I> IntoParallelIterator for RefDataset<'t, I>
where
    I: Sync + 't,
{
    type Iter = rayon::slice::Iter<'t, I>;
    type Item = &'t I;

    fn into_par_iter(self) -> Self::Iter {
        self.items.into_par_iter()
    }
}

impl<K, V> IntoParallelIterator for MapDataset<K, V>
where
    K: Hash + Eq + Send,
    V: Send,
{
    type Iter = hashbrown::hash_map::rayon::IntoParIter<K, V>;
    type Item = (K, V);

    fn into_par_iter(self) -> Self::Iter {
        self.map.into_par_iter()
    }
}

impl Dataset<PathBuf> {
    pub fn files(glob: &str) -> Result<Self, KGDataError> {
        let items = glob::glob(glob)?
            .map(|x| x.into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { items })
    }
}

impl<I> FromParallelDataset<I> for Dataset<I>
where
    I: Send,
{
    fn from_par_dataset<D>(iter: D) -> Self
    where
        D: IntoParallelIterator<Item = I>,
    {
        Self {
            items: iter.into_par_iter().collect::<Vec<_>>(),
        }
    }
}

impl<I, E> FromParallelDataset<Result<I, E>> for Result<Dataset<I>, E>
where
    I: Send,
    E: Send,
{
    fn from_par_dataset<D>(iter: D) -> Self
    where
        D: IntoParallelIterator<Item = Result<I, E>>,
    {
        let items = iter.into_par_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(Dataset { items })
    }
}

impl<I> FromIterator<I> for Dataset<I> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = I>,
    {
        Self {
            items: iter.into_iter().collect::<Vec<_>>(),
        }
    }
}
