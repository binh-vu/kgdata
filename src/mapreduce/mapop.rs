use rayon::prelude::*;

use super::{FromParallelDataset, ParallelDataset};

#[derive(Clone)]
pub struct MapOp<I, F>
where
    I: Send,
{
    pub base: &'static dyn ParallelDataset<Item = I>,
    pub op: F,
}

// #[derive(Clone)]
// pub struct MapOp<D: ParallelDataset, F> {
//     pub base: D,
//     pub op: F,
// }

#[derive(Clone)]
pub struct FlatMapOp<D: ParallelDataset, F> {
    pub base: D,
    pub op: F,
}

// impl<D, F, R> IntoParallelIterator for MapOp<D, F>
// where
//     D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     // D: ParallelDataset,
//     // F: Fn(D::Item) -> R + Sync + Send,
//     R: Send,
// {
//     type Iter = rayon::iter::Map<D::Iter, F>;
//     type Item = F::Output;

//     fn into_par_iter(self) -> Self::Iter {
//         self.base.into_par_iter().map(self.op)
//     }
// }

// impl<D, F, R> ParallelDataset for MapOp<D, F>
// where
//     // D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     // F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     D: ParallelDataset,
//     F: Fn(D::Item) -> R + Sync + Send,
//     // F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     R: Send,
// {
//     type Item = F::Output;

//     // fn collect<C>(self) -> C
//     // where
//     //     C: FromParallelDataset<Self::Item>,
//     // {
//     //     C::from_par_dataset(self)
//     // }
// }

// impl<D, F, R> ParallelDataset for MapOp<D, F>
// where
//     // D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     // F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     D: ParallelDataset,
//     F: Fn(D::Item) -> R + Sync + Send,
//     // F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     R: Send,
// {
//     type Iter = rayon::iter::Map<D::Iter, F>;
//     type Item = F::Output;

//     fn collect<C>(self) -> C
//     where
//         C: FromParallelDataset<Self::Item>,
//     {
//         C::from_par_dataset(self)
//     }
// }

// impl<D, F, R> IntoParallelIterator for FlatMapOp<D, F>
// where
//     // D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     R: IntoParallelIterator,
// {
//     type Iter = rayon::iter::FlatMap<D::Iter, F>;
//     type Item = R::Item;

//     fn into_par_iter(self) -> Self::Iter {
//         self.base.into_par_iter().flat_map(self.op)
//     }
// }

// impl<D, F, R> ParallelDataset for FlatMapOp<D, F>
// where
//     D: ParallelDataset + IntoParallelIterator<Item = <D as ParallelDataset>::Item>,
//     F: Fn(<D as ParallelDataset>::Item) -> R + Sync + Send,
//     R: IntoParallelIterator,
// {
//     type Item = R::Item;

//     fn collect(self) -> Vec<Self::Item> {
//         self.into_par_iter().collect()
//     }
// }
