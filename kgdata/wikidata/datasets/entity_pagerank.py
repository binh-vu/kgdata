from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from glob import glob
from io import BytesIO
from pathlib import Path
from typing import Callable, Generic, Iterable, List, Optional, Sequence, Tuple, TypeVar

import numpy as np
import orjson
import serde.pickle
import serde.textline
from sm.misc.ray_helper import ray_map

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.spark.extended_rdd import ExtendedRDD
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.models.wdentity import WDEntity

KeyType = TypeVar("KeyType")


@dataclass
class Edge(Generic[KeyType]):
    source: KeyType
    target: KeyType
    weight: int

    def serialize(self):
        return "\t".join((str(self.source), str(self.target), str(self.weight)))

    @staticmethod
    def deserialize_str(o: str):
        r = o.split("\t")
        return Edge(r[0], r[1], int(r[2]))

    @staticmethod
    def deserialize_int(o: str):
        r = o.split("\t")
        return Edge(int(r[0]), int(r[1]), int(r[2]))


EntityPageRank = Tuple[str, float]


@lru_cache()
def entity_pagerank() -> Dataset[EntityPageRank]:
    """Generate a weighted graph of Wikidata's entities. The graph can be used to calculate page rank to determine entity popularity"""
    cfg = WikidataDirCfg.get_instance()

    idmap_ds = Dataset(
        cfg.entity_pagerank / "idmap/*.gz",
        deserialize=kv_tab_deser,
        name="entity-pagerank/idmap",
        dependencies=[entities()],
    )
    pagerank_ds = Dataset(
        cfg.entity_pagerank / "pagerank/*.gz",
        deserialize=orjson.loads,
        name="entity-pagerank/pagerank",
        dependencies=[entities()],
    )

    if not idmap_ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .map(lambda ent: ent.id)
            .sortBy(lambda x: (x[0], int(x[1:])))  # type: ignore
            .zipWithIndex()
            .map(tab_ser)
            .save_like_dataset(
                idmap_ds,
                checksum=False,
                auto_coalesce=True,
            )
        )
        # write the total number of entity
        (cfg.entity_pagerank / "idmap.txt").write_text(str(idmap_ds.get_rdd().count()))

    edges_dataset = Dataset(
        cfg.entity_pagerank / "graph/*.gz",
        deserialize=Edge.deserialize_int,
        name="entity-pagerank/graph",
        dependencies=[entities(), idmap_ds],
    )
    if not edges_dataset.has_complete_data():
        idmap_rdd = idmap_ds.get_extended_rdd()
        (
            entities()
            .get_extended_rdd()
            .flatMap(get_edges)
            .map(lambda x: (x.source, x))
            .groupByKey()
            .leftOuterJoin(idmap_rdd)
            .flatMap(update_edge_ids_source)
            .map(lambda x: (x.target, x))
            .left_outer_join_repartition(
                idmap_rdd,
                num_partitions=3000,
            )
            .flatMap(update_edge_ids_target)
            .map(Edge.serialize)
            .save_like_dataset(edges_dataset, checksum=False)
        )

    graphtool_indir = cfg.entity_pagerank / f"graphtool"
    if not does_result_dir_exist(graphtool_indir, create_if_not_exist=True):

        def create_edges_npy(infiles: List[str], outfile: str):
            edges = []
            eprops = []
            for infile in infiles:
                for x in serde.textline.deser(infile, trim=True):
                    edge = Edge.deserialize_int(x)
                    edges.append((edge.source, edge.target))
                    eprops.append(edge.weight)

            edges = np.asarray(edges)
            eprops = np.asarray(eprops)
            np.savez_compressed(outfile, edges=edges, eprops=eprops)

        # leverage the fact that input file has the format: `part-00000.gz`
        infiles = edges_dataset.get_files()
        outfiles = {}
        for infile in infiles:
            outfile = str(graphtool_indir / (Path(infile).stem[:-1] + ".npz"))
            if outfile not in outfiles:
                outfiles[outfile] = []
            outfiles[outfile].append(infile)
        assert sum(len(x) for x in outfiles.values()) == len(infiles)

        ray_map(
            create_edges_npy,
            [(sub_infiles, outfile) for outfile, sub_infiles in outfiles.items()],
            verbose=True,
            poll_interval=0.5,
            is_func_remote=False,
        )
        (graphtool_indir / "_SUCCESS").touch()

    if not pagerank_ds.has_complete_data():
        assert does_result_dir_exist(
            cfg.entity_pagerank / "graphtool_pagerank", allow_override=False
        ), "Must run graph-tool pagerank at `kgdata/scripts/pagerank_v2.py` first"

        n_files = len(glob(str(cfg.entity_pagerank / "graphtool_pagerank" / "*.npz")))

        def deserialize_np(dat: bytes) -> List[Tuple[int, float]]:
            f = BytesIO(dat)
            array = np.load(f)
            return list(zip(array["ids"], array["data"]))

        def process_join(
            x: Tuple[int, Tuple[Optional[float], Optional[str]]]
        ) -> Tuple[str, float]:
            assert x[1][0] is not None
            assert x[1][1] is not None
            return x[1][1], float(x[1][0])

        (
            ExtendedRDD.binaryFiles(
                cfg.entity_pagerank / "graphtool_pagerank" / "*.npz",
            )
            .repartition(n_files)
            .flatMap(lambda x: deserialize_np(x[1]))
            .fullOuterJoin(idmap_ds.get_extended_rdd().map(lambda x: (int(x[1]), x[0])))
            .map(process_join)
            .map(orjson.dumps)
            .save_like_dataset(pagerank_ds, trust_dataset_dependencies=True)
        )

    pagerank_stat_outfile = cfg.entity_pagerank / f"pagerank.pkl"
    if not pagerank_stat_outfile.exists():
        n_files = len(glob(str(cfg.entity_pagerank / "graphtool_pagerank" / "*.npz")))

        def deserialize_np2(dat: bytes) -> np.ndarray:
            f = BytesIO(dat)
            array = np.load(f)
            return array["data"]

        rdd = (
            get_spark_context()
            .binaryFiles(
                str(cfg.entity_pagerank / "graphtool_pagerank" / "*.npz"),
            )
            .repartition(n_files)
            .map(lambda x: deserialize_np2(x[1]))
        )

        total = rdd.map(lambda x: np.sum(x)).sum()  # type: ignore
        size = rdd.map(lambda x: len(x)).sum()
        mean_pagerank = total / size
        std_pagerank = np.sqrt(
            rdd.map(lambda x: np.sum(np.square(x - mean_pagerank))).sum() / size
        )
        max_pagerank = rdd.map(lambda x: np.max(x)).max()  # type: ignore
        min_pagerank = rdd.map(lambda x: np.min(x)).min()  # type: ignore

        sumlog = rdd.map(lambda x: np.sum(np.log(x))).sum()
        meanlog = sumlog / size
        stdlog = np.sqrt(
            rdd.map(lambda x: np.sum(np.square(np.log(x) - meanlog))).sum() / size
        )

        serde.pickle.ser(
            {
                "sum": float(total),
                "len": int(size),
                "mean": float(mean_pagerank),
                "max": float(max_pagerank),
                "min": float(min_pagerank),
                "std": float(std_pagerank),
                "meanlog": float(meanlog),
                "stdlog": float(stdlog),
            },
            pagerank_stat_outfile,
        )

    return pagerank_ds


def get_edges(ent: WDEntity) -> List[Edge]:
    edges = defaultdict(int)
    for pid, stmts in ent.props.items():
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                edges[ent.id, stmt.value.as_entity_id()] += 1
            for qvals in stmt.qualifiers.values():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        edges[ent.id, qval.as_entity_id()] += 1

    return [Edge(source=s, target=t, weight=w) for (s, t), w in edges.items()]


def update_edge_ids_source(
    joined_result: Tuple[str, Tuple[Iterable[Edge], Optional[str]]]
):
    assert joined_result[1][1] is not None
    for edge in joined_result[1][0]:
        edge.source = joined_result[1][1]
    return joined_result[1][0]


def update_edge_ids_target(
    joined_result: Tuple[str, Tuple[Iterable[Edge], Optional[str]]]
):
    assert joined_result[1][1] is not None
    for edge in joined_result[1][0]:
        edge.target = joined_result[1][1]
    return joined_result[1][0]


def tab_ser(a: Sequence):
    return "\t".join((str(x) for x in a))


def tab_deser(o: str):
    return o.split("\t")


kv_tab_deser: Callable[[str], Tuple[str, str]] = tab_deser  # type: ignore
