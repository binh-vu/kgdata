import os, glob, numpy as np
from pathlib import Path

import serde.textline
from graph_tool.all import *
from tqdm import tqdm
from loguru import logger

wd_dir = Path(os.environ["WD_DIR"])
working_dir = wd_dir / "entity_pagerank"

edge_files = sorted(glob.glob(str((working_dir / "graph_en" / "*.gz"))))

if (working_dir / "graph_en.gt").exists():
    logger.info("Loading graph...")
    g = Graph(directed=True)
    g.load(str(working_dir / "graph_en.gt"), fmt="gt")
    logger.info("Loading graph... done!")
else:
    logger.info("Loading graph data...")
    n_vertices = int((working_dir / "idmap_en.txt").read_text())
    edges = []
    for file in tqdm(edge_files):
        for line in serde.textline.deser(file, trim=True):
            edges.append([int(x) for x in line.split("\t")])
    logger.info("Loading graph data... done!")

    logger.info("Creating graph...")
    g = Graph(directed=True)
    g.add_vertex(n=n_vertices)
    logger.info("Creating graph... added vertices")
    eweight = g.new_ep("int")
    g.add_edge_list(edges, eprops=[eweight])
    g.edge_properties["weight"] = eweight
    logger.info("Creating graph... done!")

    logger.info("Saving graph...")
    g.save(str(working_dir / "graph_en.gt"), fmt="gt")
    logger.info("Saving graph... done!")

logger.info("Calculating pagerank...")
ranks = pagerank(g, weight=g.edge_properties["weight"])
logger.info("Calculating pagerank... done!")

logger.info("Saving pagerank...")
np.save(str(working_dir / "gt_pagerank.npy"), ranks.a)
logger.info("Saving pagerank... done!")
