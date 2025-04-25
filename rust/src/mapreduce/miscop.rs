use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct TakeAny<D: ParallelDataset> {
    pub base: D,
    pub n: usize,
}

impl<D> IntoParallelIterator for TakeAny<D>
where
    D: ParallelDataset,
{
    type Iter = rayon::iter::TakeAny<D::Iter>;
    type Item = D::Item;

    fn into_par_iter(self) -> Self::Iter {
        self.base.into_par_iter().take_any(self.n)
    }
}

impl<D> ParallelDataset for TakeAny<D> where D: ParallelDataset {}
