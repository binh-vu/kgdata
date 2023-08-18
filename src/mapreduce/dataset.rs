use std::path::PathBuf;

use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use crate::error::KGDataError;

use super::{FromParallelDataset, ParallelDataset};

pub struct Dataset<I> {
    items: Vec<I>,
}

impl<I> ParallelDataset for Dataset<I>
where
    I: Send,
{
    type Item = I;

    // fn collect<C>(self) -> C
    // where
    //     C: FromParallelDataset<Self::Item>,
    // {
    //     C::from_par_dataset(self)
    // }
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
