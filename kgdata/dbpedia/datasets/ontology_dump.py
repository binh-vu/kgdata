from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

import orjson

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDataDirCfg
from kgdata.misc.ntriples_parser import ignore_comment, ntriple_loads
from kgdata.misc.resource import Resource
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_file


def ontology_dump() -> Dataset[Resource]:
    """
    Split the DBpedia ontology dump into smaller files.

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDataDirCfg.get_instance()

    outdir = cfg.ontology_dump / "step2"

    if not does_result_dir_exist(outdir):
        infile = cfg.get_ontology_dump_file()
        step1_dir = cfg.ontology_dump / "step1"
        split_a_file(
            infile=infile,
            outfile=step1_dir / f"part{infile.suffix}.gz",
            n_writers=8,
            override=False,
            n_records_per_file=5000,
        )

        (
            get_spark_context()
            .textFile(str(step1_dir / "*.gz"))
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(Resource.ser)
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset.string(file_pattern=str(outdir / "*.gz")).map(Resource.deser)


def aggregated_triples(val: tuple[str, Iterable[tuple[str, str, str]]]) -> Resource:
    source, pred_objs = val
    props: dict[str, list] = defaultdict(list)
    for _, pred, obj in pred_objs:
        props[pred].append(obj)
    return Resource(source, dict(props))
