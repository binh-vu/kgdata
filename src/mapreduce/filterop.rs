use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct FilterOp<D: ParallelDataset, F> {
    pub base: D,
    pub op: F,
}

impl<D, F> IntoParallelIterator for FilterOp<D, F>
where
    D: ParallelDataset,
    F: (Fn(&D::Item) -> bool) + Sync + Send,
{
    type Iter = rayon::iter::Filter<D::Iter, F>;
    type Item = D::Item;

    fn into_par_iter(self) -> Self::Iter {
        self.base.into_par_iter().filter(self.op)
    }
}

impl<D, F> ParallelDataset for FilterOp<D, F>
where
    D: ParallelDataset,
    F: (Fn(&D::Item) -> bool) + Sync + Send,
{
}
