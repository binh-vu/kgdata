from bz2 import BZ2File
from functools import lru_cache
from gzip import GzipFile
from io import BufferedReader
from typing import BinaryIO, Union

import orjson

from kgdata.dataset import Dataset
from kgdata.splitter import split_a_file
from kgdata.wikidata.config import WikidataDirCfg


@lru_cache()
def entity_dump() -> Dataset[dict]:
    """
    Split the Wikidata entity dumps into smaller files.

    Returns:
        Dataset[dict]
    """
    cfg = WikidataDirCfg.get_instance()
    dump_date = cfg.get_dump_date()
    ds = Dataset(
        file_pattern=cfg.entity_dump / "*.zst",
        deserialize=orjson.loads,
        name=f"entity-dump/{dump_date}",
        dependencies=[],
    )

    if not ds.has_complete_data():
        split_a_file(
            infile=cfg.get_entity_dump_file(),
            outfile=cfg.entity_dump / "part.ndjson.zst",
            record_iter=_record_iter,
            record_postprocess="kgdata.wikidata.datasets.entity_dump._record_postprocess",
            n_writers=8,
            override=False,
            compression_level=9,
        )
        ds.sign(ds.get_name(), ds.get_dependencies())
    return ds


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
    WikidataDirCfg.init("/var/tmp/kgdata/wikidata/20230619")
    print(entity_dump().get_extended_rdd().count())
