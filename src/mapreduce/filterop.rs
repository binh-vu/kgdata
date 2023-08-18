use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct FilterOp<D: ParallelDataset, F> {
    pub base: D,
    pub op: F,
}

impl<D, F> IntoParallelIterator for FilterOp<D, F>
where
    D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
    F: Fn(&<D as ParallelDataset>::Item) -> bool + Sync + Send,
{
    type Iter = rayon::iter::Filter<D::Iter, F>;
    type Item = <D as ParallelDataset>::Item;

    fn into_par_iter(self) -> Self::Iter {
        self.base.into_par_iter().filter(self.op)
    }
}

impl<D, F> ParallelDataset for FilterOp<D, F>
where
    D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
    F: Fn(&<D as ParallelDataset>::Item) -> bool + Sync + Send,
{
    type Item = <D as ParallelDataset>::Item;

    fn collect(self) -> Vec<Self::Item> {
        self.into_par_iter().collect()
    }
}
