use core::hash::Hash;
use hashbrown::HashMap;
use rayon::prelude::*;

pub mod dataset;
pub mod filterop;
pub mod foldop;
pub mod functions;
pub mod mapop;
pub mod miscop;
pub mod sortop;

pub use self::dataset::*;
pub use self::filterop::*;
pub use self::functions::*;
pub use self::mapop::*;
pub use self::miscop::*;
pub use self::sortop::*;

/// A note on the implementation: due to the trait methods required Sized on most of the methods,
/// if we use as trait object, we can't use most of its methods. To prevent early boxing error, we required
/// it to be Sized.
pub trait ParallelDataset: Sized + Send + IntoParallelIterator {
    fn map<F, R>(self, op: F) -> self::mapop::MapOp<Self, F>
    where
        F: (Fn(Self::Item) -> R) + Sync + Send,
        R: Send,
    {
        self::mapop::MapOp { base: self, op }
    }

    fn flat_map<F, R>(self, op: F) -> self::mapop::FlatMapOp<Self, F>
    where
        F: (Fn(Self::Item) -> R) + Sync + Send,
        R: IntoParallelIterator,
    {
        self::mapop::FlatMapOp { base: self, op }
    }

    fn filter<F>(self, op: F) -> self::filterop::FilterOp<Self, F>
    where
        F: Fn(&Self::Item) -> bool + Sync,
    {
        self::filterop::FilterOp { base: self, op }
    }

    fn fold<T, ID, F>(self, identity: ID, op: F) -> self::foldop::FoldOp<Self, ID, F>
    where
        F: (Fn(T, Self::Item) -> T) + Sync + Send,
        ID: Fn() -> T + Sync + Send,
        T: Send,
    {
        self::foldop::FoldOp {
            base: self,
            identity,
            op,
        }
    }

    fn reduce<ID, F>(self, identity: ID, op: F) -> Self::Item
    where
        F: (Fn(Self::Item, Self::Item) -> Self::Item) + Sync + Send,
        ID: (Fn() -> Self::Item) + Sync + Send,
    {
        self.into_par_iter().reduce(identity, op)
    }

    fn sort_by_key<F, K>(self, op: F, ascending: bool) -> self::sortop::SortByKeyOp<Self, F>
    where
        F: Fn(&Self::Item) -> K + Sync,
        K: Ord + Send,
    {
        self::sortop::SortByKeyOp {
            base: self,
            op,
            ascending,
        }
    }

    fn group_by<F, K>(self, key: F) -> MapDataset<K, Vec<Self::Item>>
    where
        F: Fn(&Self::Item) -> K + Sync,
        K: Hash + Eq + Send,
    {
        let map = self
            .fold(
                HashMap::new,
                |mut map: HashMap<K, Vec<Self::Item>>, item: Self::Item| {
                    map.entry(key(&item)).or_default().push(item);
                    map
                },
            )
            .reduce(HashMap::new, merge_map_list);

        MapDataset { map }
    }

    fn group_by_map<K, V, F1, F2>(self, key: F1, value: F2) -> MapDataset<K, Vec<V>>
    where
        F1: Fn(&Self::Item) -> K + Sync,
        F2: Fn(&Self::Item) -> V + Sync,
        K: Hash + Eq + Send,
        V: Send,
    {
        let map = self
            .fold(
                HashMap::new,
                |mut map: HashMap<K, Vec<V>>, item: Self::Item| {
                    map.entry(key(&item)).or_default().push(value(&item));
                    map
                },
            )
            .reduce(HashMap::new, merge_map_list);

        MapDataset { map }
    }

    fn count(self) -> usize {
        self.into_par_iter().count()
    }

    fn take_any(self, n: usize) -> TakeAny<Self> {
        TakeAny { base: self, n }
    }

    fn collect<C>(self) -> C
    where
        C: FromParallelDataset<Self::Item>,
    {
        C::from_par_dataset(self)
    }
}

pub trait FromParallelDataset<I> {
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: ParallelDataset<Item = I>;
}

impl<I> FromParallelDataset<I> for Vec<I>
where
    I: Send,
{
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: ParallelDataset<Item = I>,
    {
        dataset.into_par_iter().collect()
    }
}

impl<K, V> FromParallelDataset<(K, V)> for HashMap<K, V>
where
    K: Hash + Eq + Send,
    V: Send,
{
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: ParallelDataset<Item = (K, V)>,
    {
        dataset.into_par_iter().collect()
    }
}

impl<I, E> FromParallelDataset<Result<I, E>> for Result<Vec<I>, E>
where
    I: Send,
    E: Send,
{
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: ParallelDataset<Item = Result<I, E>>,
    {
        dataset.into_par_iter().collect::<Result<Vec<I>, E>>()
    }
}

impl<K, V, E> FromParallelDataset<Result<(K, V), E>> for Result<HashMap<K, V>, E>
where
    K: Hash + Eq + Send,
    V: Send,
    E: Send,
{
    fn from_par_dataset<D>(dataset: D) -> Self
    where
        D: ParallelDataset<Item = Result<(K, V), E>>,
    {
        dataset.into_par_iter().collect()
    }
}

pub trait IntoParallelDataset {
    type Dataset: ParallelDataset<Item = Self::Item>;
    type Item: Send;

    fn into_par_dataset(self) -> Self::Dataset;
}
