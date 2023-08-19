use rayon::prelude::*;

use super::ParallelDataset;

#[derive(Clone)]
pub struct SortByKeyOp<D: ParallelDataset, F> {
    pub base: D,
    pub op: F,
    pub ascending: bool,
}

// impl<D, F> ParallelDataset for SortByKeyOp<D, F>
// where
//     D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     F: Fn(&<D as ParallelDataset>::Item) -> bool + Sync + Send,
// {
//     type Item = <D as ParallelDataset>::Item;

//     fn collect(self) -> Vec<Self::Item> {
//         let mut items: Vec<Self::Item> = self.base.into_par_iter().collect();
//         if self.ascending {
//             items.sort_unstable_by_key(self.op);
//         } else {
//             items.sort_unstable_by_key(|item| std::cmp::Reverse((self.op)(item)));
//         }

//         items
//     }
// }
