from bz2 import BZ2File
import glob
from gzip import GzipFile
import os
from pathlib import Path
from typing import BinaryIO, Union, cast
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.dataset import Dataset

import orjson
from kgdata.spark import get_spark_context
from kgdata.splitter import split_a_file
from pyspark import RDD


def entity_dump() -> Dataset[dict]:
    """
    Split the Wikidata entity dumps into smaller files.

    Returns:
        Dataset[dict]
    """
    cfg = WDDataDirCfg.get_instance()

    split_a_file(
        infile=cfg.get_entity_dump_file(),
        outfile=cfg.entity_dump / "part.ndjson.gz",
        record_iter=_record_iter,
        record_postprocess="kgdata.wikidata.datasets.entity_dumps._record_postprocess",
        n_writers=8,
        override=False,
    )

    return Dataset(file_pattern=str(cfg.entity_dump / "*.gz"), deserialize=orjson.loads)


def _record_iter(f: Union[BZ2File, GzipFile, BinaryIO]):
    line = f.readline()[:-1]
    assert line == b"["
    return f


def _record_postprocess(record: str):
    if record[-3:] == b"},\n":
        return record[:-2]
    if record[-2:] == b"}\n":
        return record[:-1]
    if record == b"]\n":
        return None
    raise Exception("Unreachable! Get a record: {}".format(record))


if __name__ == "__main__":
    entity_dump()
