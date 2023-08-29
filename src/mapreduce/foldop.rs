use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct FoldOp<D: ParallelDataset, ID, F> {
    pub base: D,
    pub identity: ID,
    pub op: F,
}

impl<T, D, ID, F> IntoParallelIterator for FoldOp<D, ID, F>
where
    D: ParallelDataset,
    F: (Fn(T, D::Item) -> T) + Sync + Send,
    ID: Fn() -> T + Sync + Send,
    T: Send,
{
    type Iter = rayon::iter::Fold<D::Iter, ID, F>;
    type Item = T;

    fn into_par_iter(self) -> Self::Iter {
        self.base.into_par_iter().fold(self.identity, self.op)
    }
}

impl<D, T, ID, F> ParallelDataset for FoldOp<D, ID, F>
where
    D: ParallelDataset,
    F: (Fn(T, D::Item) -> T) + Sync + Send,
    ID: Fn() -> T + Sync + Send,
    T: Send,
{
}
