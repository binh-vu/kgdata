use crate::mapreduce::ParallelDataset;

pub struct PyDataset {
    it: Box<dyn ParallelDataset>,
}

impl PyDataset {}
