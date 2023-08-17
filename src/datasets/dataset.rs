// use super::computing_graph::ComputingGraph;
// use serde::{Deserialize, Serialize};

use super::mapreduce::ParallelDataset;

// #[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Dataset {
    // pattern to match files
    filepattern: String,
}
