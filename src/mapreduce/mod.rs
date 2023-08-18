pub mod filterop;
pub mod functions;
pub mod mapop;
pub mod sortop;

pub trait ParallelDataset: Sized + Send {
    /// Type of item of the dataset
    type Item: Send;

    fn map<F, R>(self, op: F) -> self::mapop::MapOp<Self, F>
    where
        F: Fn(Self::Item) -> R + Sync,
        R: Send,
        Self: Sized,
    {
        self::mapop::MapOp { base: self, op }
    }

    // fn flat_map<F, R>

    fn filter<F>(self, op: F) -> self::filterop::FilterOp<Self, F>
    where
        F: Fn(&Self::Item) -> bool + Sync,
        Self: Sized,
    {
        self::filterop::FilterOp { base: self, op }
    }

    fn sort_by_key<F, K>(self, op: F, ascending: bool) -> self::sortop::SortByKeyOp<Self, F>
    where
        F: Fn(&Self::Item) -> K + Sync,
        K: Ord + Send,
        Self: Sized,
    {
        self::sortop::SortByKeyOp {
            base: self,
            op,
            ascending,
        }
    }

    fn collect(self) -> Vec<Self::Item>;

    fn take(self, n: usize) -> Vec<Self::Item> {
        let mut res = self.collect();
        res.truncate(n);
        res
    }
}

pub struct Dataset<I> {
    items: Vec<I>,
}

impl<I> ParallelDataset for Dataset<I>
where
    I: Send,
{
    type Item = I;

    fn collect(self) -> Vec<Self::Item> {
        self.items
    }
}
