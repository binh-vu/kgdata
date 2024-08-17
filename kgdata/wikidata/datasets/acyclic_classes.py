from __future__ import annotations

from functools import partial

import pandas as pd
import rustworkx
from graph.retworkx import BaseEdge, BaseNode, RetworkXStrDiGraph
from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.misc.hierarchy import build_ancestors
from kgdata.splitter import split_a_list
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.db import get_class_db
from kgdata.wikidata.models.wdclass import WDClass
from loguru import logger


def acyclic_classes(lang: str = "en", with_deps: bool = True):
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.acyclic_classes / f"{lang}/*.gz",
        deserialize=partial(deser_from_dict, WDClass),
        name=f"acyclic_classes/{lang}",
        dependencies=[classes(lang)] if with_deps else [],
    )

    if not ds.has_complete_data():
        assert with_deps, "Dependencies are required to generate acyclic classes"

        records = classes(lang).get_list()

        # create a graph
        g = RetworkXStrDiGraph(check_cycle=False, multigraph=False)
        for c in records:
            g.add_node(BaseNode(c.id))
        for c in records:
            for cpid in c.parents:
                g.add_edge(BaseEdge(-1, c.id, cpid, 1))

        cycles = all_cycles(g)
        logger.info("Find {} cycles", len(cycles))

        # we first look at the latest version of wikidata and remove links that are not in the latest version
        clsdb = get_class_db(
            cfg.acyclic_classes / "classes.db", read_only=False, proxy=True
        )

        del_edges = []
        for cycle in cycles:
            id2c = {uid: clsdb[uid] for uid in cycle}
            for cid, c in id2c.items():
                # detect what should remove
                old_parents = set([e.target for e in g.out_edges(c.id)])
                del_edges.extend(
                    [(cid, cpid) for cpid in old_parents.difference(c.parents)]
                )

        logger.info("Remove {} edges", len(del_edges))
        for edge in del_edges:
            g.remove_edges_between_nodes(edge[0], edge[1])

        # after that, we just pick the class with more parents to remove
        new_cycles = all_cycles(g)
        logger.info("Find {} cycles", len(new_cycles))

        all_guess_del_edges = []
        while True:
            guess_del_edges = []
            for cycle in new_cycles:
                edges = []
                for uid in cycle:
                    for vid in cycle:
                        if g.has_edges_between_nodes(uid, vid):
                            edges.append((uid, vid))
                guess_del_edges.append(
                    max(edges, key=lambda x: len(clsdb[x[1]].parents))
                )

            for edge in guess_del_edges:
                g.remove_edges_between_nodes(edge[0], edge[1])

            logger.info("Remove {} edges", len(guess_del_edges))
            all_guess_del_edges.extend(guess_del_edges)
            new_cycles = all_cycles(g)
            if len(new_cycles) == 0:
                break
            logger.info("Find {} cycles", len(new_cycles))

        # write the result
        pd.DataFrame([{"source": s, "target": t} for s, t in del_edges]).to_csv(
            cfg.acyclic_classes / "del_edges.csv", index=False
        )
        pd.DataFrame(
            [{"source": s, "target": t} for s, t in all_guess_del_edges]
        ).to_csv(cfg.acyclic_classes / "guess_del_edges.csv", index=False)

        id2record = {c.id: c for c in records}
        for uid, vid in del_edges + all_guess_del_edges:
            id2record[uid].parents.remove(vid)

        logger.info("Build ancestors")
        build_ancestors(records)
        split_a_list(
            [ser_to_dict(c) for c in records],
            ds.get_data_directory() / "part.jl.gz",
        )
        ds.sign(ds.get_name(), ds.get_dependencies())

    return ds


def all_cycles(g):
    out = []
    for nodeindices in rustworkx.simple_cycles(g._graph):
        cycle = []
        for uid in nodeindices:
            cycle.append(g._graph.get_node_data(uid).id)
        out.append(cycle)
    return out
