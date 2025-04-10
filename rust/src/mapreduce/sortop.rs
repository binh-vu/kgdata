use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct SortByKeyOp<D: ParallelDataset, F> {
    pub base: D,
    pub op: F,
    pub ascending: bool,
}

impl<D, F> SortByKeyOp<D, F>
where
    D: ParallelDataset,
    F: Fn(&D::Item) -> bool + Sync + Send,
{
    #[allow(dead_code)]
    fn collect(self) -> Vec<D::Item> {
        let mut items: Vec<D::Item> = self.base.into_par_iter().collect();
        if self.ascending {
            items.sort_unstable_by_key(self.op);
        } else {
            items.sort_unstable_by_key(|item| std::cmp::Reverse((self.op)(item)));
        }

        items
    }
}
