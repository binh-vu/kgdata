import os, glob, numpy as np
from pathlib import Path

from sm.misc.deser import deserialize_lines
from graph_tool.all import *
from tqdm import tqdm
from loguru import logger

g = Graph(directed=True)
g.add_vertex(n=10)

edges1 = np.asarray(
    [
        [0, 1],
        [1, 2],
        [2, 3],
        [3, 4],
    ]
)
edges2 = np.asarray(
    [
        [5, 6],
        [6, 7],
        [7, 8],
    ]
)
weights1 = np.asarray([1, 3, 5, 7])
weights2 = np.asarray([11, 13, 17])

batch = [(edges1, weights1), (edges2, weights2)]

weights = []
for x, y in reversed(batch):
    g.add_edge_list(x)
    weights.append(y)

eweight = g.new_ep("int", vals=np.concatenate(weights))
for e in g.edges():
    print(e, eweight[e])
