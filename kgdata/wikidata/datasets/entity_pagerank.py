from collections import defaultdict
from dataclasses import dataclass
from glob import glob
from io import BytesIO
from pathlib import Path
import shutil
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from hugedict.misc import identity
from hugedict.ray_parallel import ray_map
from kgdata.dataset import Dataset
from kgdata.spark import (
    does_result_dir_exist,
    left_outer_join_repartition,
    get_spark_context,
    left_outer_join,
)
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from loguru import logger
import orjson, ray, numpy as np
from sm.misc.deser import deserialize_byte_lines, deserialize_lines
from tqdm import tqdm


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


def entity_pagerank(lang: str = "en") -> Dataset[EntityPageRank]:
    """Generate a weighted graph of Wikidata's entities. The graph can be used to calculate page rank to determine entity popularity"""
    cfg = WDDataDirCfg.get_instance()

    idmap_outdir = cfg.entity_pagerank / f"idmap_{lang}"
    if not does_result_dir_exist(idmap_outdir):
        (
            entities(lang=lang)
            .get_rdd()
            .map(lambda ent: ent.id)
            .sortBy(lambda x: (x[0], int(x[1:])))  # type: ignore
            .zipWithIndex()
            .map(tab_ser)
            .saveAsTextFile(
                str(idmap_outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )
        # write the total number of entity
        (cfg.entity_pagerank / (idmap_outdir.name + ".txt")).write_text(
            str(
                Dataset(
                    idmap_outdir / "*.gz",
                    deserialize=kv_tab_deser,
                )
                .get_rdd()
                .count()
            )
        )

    entity_idmap = Dataset(
        idmap_outdir / "*.gz",
        deserialize=kv_tab_deser,
    )

    graph_outdir = cfg.entity_pagerank / f"graph_{lang}"
    if not does_result_dir_exist(graph_outdir):
        idmap_rdd = entity_idmap.get_rdd()
        (
            left_outer_join_repartition(
                entities(lang=lang)
                .get_rdd()
                .flatMap(get_edges)
                .map(lambda x: (x.source, x))
                .groupByKey()
                .leftOuterJoin(idmap_rdd)
                .flatMap(update_edge_ids_source)
                .map(lambda x: (x.target, x)),
                idmap_rdd,
                num_partitions=3000,
            )
            .flatMap(update_edge_ids_target)
            .map(Edge.serialize)
            .saveAsTextFile(
                str(graph_outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    edges_dataset: Dataset[Edge[int]] = Dataset(
        graph_outdir / "*.gz",
        deserialize=Edge.deserialize_int,
    )

    graphtool_indir = cfg.entity_pagerank / f"graphtool_{lang}"
    if not does_result_dir_exist(graphtool_indir, create_if_not_exist=True):
        ray.init()

        @ray.remote
        def create_edges_npy(infiles: List[str], outfile: str):
            edges = []
            eprops = []
            for infile in infiles:
                for x in deserialize_lines(infile, trim=True):
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
            create_edges_npy.remote,
            [(sub_infiles, outfile) for outfile, sub_infiles in outfiles.items()],
            verbose=True,
            poll_interval=0.5,
        )
        (graphtool_indir / "_SUCCESS").touch()

    pagerank_outdir = cfg.entity_pagerank / f"pagerank_{lang}"
    if not does_result_dir_exist(pagerank_outdir):
        assert does_result_dir_exist(
            cfg.entity_pagerank / "graphtool_pagerank_en", allow_override=False
        ), "Must run graph-tool pagerank at `kgdata/scripts/pagerank_v2.py` first"

        n_files = len(
            glob(str(cfg.entity_pagerank / "graphtool_pagerank_en" / "*.npz"))
        )

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
            get_spark_context()
            .binaryFiles(
                str(cfg.entity_pagerank / "graphtool_pagerank_en" / "*.npz"),
            )
            .repartition(n_files)
            .flatMap(lambda x: deserialize_np(x[1]))
            .fullOuterJoin(entity_idmap.get_rdd().map(lambda x: (int(x[1]), x[0])))
            .map(process_join)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(pagerank_outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(pagerank_outdir / "*.gz", deserialize=orjson.loads)


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
