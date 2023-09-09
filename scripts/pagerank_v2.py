import glob
import math
import os
import shutil
from pathlib import Path

import numpy as np
from graph_tool.all import *
from loguru import logger
from tqdm.auto import tqdm

wd_dir = Path(os.environ["WD_DIR"])
working_dir = wd_dir / "074_entity_pagerank"
pagerank_outdir = working_dir / "graphtool_pagerank"
edge_files = sorted(glob.glob(str((working_dir / "graphtool" / "part-*.npz"))))

logger.info("Creating graph from data...")
g = Graph(directed=True)
n_vertices = int((working_dir / "idmap.txt").read_text())
g.add_vertex(n=n_vertices)
logger.info("Creating graph... added vertices")

weights = []
for file in tqdm(edge_files):
    dat = np.load(file)
    edges = dat["edges"]
    weights.append(dat["eprops"])
    g.add_edge_list(edges)

eweight = g.new_ep("int", vals=np.concatenate(weights))
logger.info("Creating graph... done!")

logger.info("Calculating pagerank...")
ranks = pagerank(g, weight=eweight)
logger.info("Calculating pagerank... done!")

# import IPython

# IPython.embed()

logger.info("Saving pagerank...")
n_records_per_file = 64000
if (pagerank_outdir).exists():
    shutil.rmtree(str(pagerank_outdir))
pagerank_outdir.mkdir(parents=True)

data = ranks.a
n_records = data.shape[0]
for no, i in tqdm(
    enumerate(range(0, n_records, n_records_per_file)),
    desc="Saving pagerank...",
    total=math.ceil(n_records / n_records_per_file),
):
    batch = data[i : i + n_records_per_file]
    np.savez_compressed(
        pagerank_outdir / f"part-{no:05d}.npz",
        data=batch,
        ids=np.arange(i, i + batch.shape[0]),
    )
pagerank_outdir.joinpath("_SUCCESS").touch()
logger.info("Saving pagerank... done!")
