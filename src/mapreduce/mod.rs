use rayon::prelude::*;

pub mod dataset;
// pub mod filterop;
// pub mod functions;
pub mod mapop;
// pub mod sortop;

// pub use self::filterop::*;
// pub use self::functions::*;
pub use self::mapop::*;
// pub use self::sortop::*;
pub use self::dataset::*;

/// A note on the implementation: due to the trait methods required Sized on most of the methods,
/// if we use as trait object, we can't use most of its methods. To prevent early boxing error, we required
/// it to be Sized.
pub trait ParallelDataset: Send {
    // type Iter: ParallelIterator<Item = Self::Item>;
    type Item: Send;

    fn map<F, R>(self, op: F) -> self::mapop::MapOp<Self::Item, F>
    where
        F: Fn(Self::Item) -> R + Sync,
        R: Send;
    // {
    //     self::mapop::MapOp { base: self, op }
    // }

    // fn flat_map<F, R>(self, op: F) -> self::mapop::FlatMapOp<Self, F>
    // where
    //     F: Fn(Self::Item) -> R,
    //     R: IntoParallelIterator + Sync + Send,
    // {
    //     self::mapop::FlatMapOp { base: self, op }
    // }

    // fn filter<F>(self, op: F) -> self::filterop::FilterOp<Self, F>
    // where
    //     F: Fn(&Self::Item) -> bool + Sync,
    //     Self: Sized,
    // {
    //     self::filterop::FilterOp { base: self, op }
    // }

    // fn sort_by_key<F, K>(self, op: F, ascending: bool) -> self::sortop::SortByKeyOp<Self, F>
    // where
    //     F: Fn(&Self::Item) -> K + Sync,
    //     K: Ord + Send,
    //     Self: Sized,
    // {
    //     self::sortop::SortByKeyOp {
    //         base: self,
    //         op,
    //         ascending,
    //     }
    // }

    // fn collect<C>(self) -> C
    // where
    //     C: FromParallelDataset<Self::Item>,
    // {
    //     C::from_par_dataset(self)
    // }

    // fn take(self, n: usize) -> Vec<Self::Item>
    // where
    //     Self: Sized,
    // {
    //     let mut res = self.collect::<Vec<_>>();
    //     res.truncate(n);
    //     res
    // }
}

pub trait FromParallelDataset<I> {
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: IntoParallelIterator<Item = I>;
}

impl<I> FromParallelDataset<I> for Vec<I>
where
    I: Send,
{
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: IntoParallelIterator<Item = I>,
    {
        dataset.into_par_iter().collect()
    }
}
