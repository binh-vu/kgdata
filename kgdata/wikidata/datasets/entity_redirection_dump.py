from bz2 import BZ2File
from functools import lru_cache
from gzip import GzipFile
from typing import BinaryIO, Union

from kgdata.dataset import Dataset
from kgdata.splitter import split_a_file
from kgdata.wikidata.config import WikidataDirCfg


@lru_cache()
def entity_redirection_dump() -> Dataset[str]:
    """Wikidata entity redirections"""
    cfg = WikidataDirCfg.get_instance()
    dump_date = cfg.get_dump_date()

    ds = Dataset.string(
        cfg.entity_redirection_dump / "*.gz",
        name=f"entity-redirection-dump/{dump_date}",
        dependencies=[],
    )

    if not ds.has_complete_data():
        split_a_file(
            infile=cfg.get_redirect_dump_file(),
            outfile=cfg.entity_redirection_dump / "part.sql.gz",
            record_iter=_record_iter,
            record_postprocess="kgdata.wikidata.datasets.entity_redirection_dump._record_postprocess",
            n_writers=8,
            override=False,
        )
        ds.sign(ds.get_name(), ds.get_dependencies())

    return ds


def _record_iter(f: Union[BZ2File, GzipFile, BinaryIO]):
    for line in f:
        if line.startswith(b"INSERT INTO"):
            yield line
            break
    for line in f:
        yield line


def _record_postprocess(line: bytes):
    if not line.startswith(b"INSERT INTO"):
        assert line.startswith(b"/*") or line == b"\n" or line.startswith(b"--"), line
        return None
    return line.rstrip(b"\r\n")


if __name__ == "__main__":
    WikidataDirCfg.init("~/kgdata/wikidata/20211213")
    entity_redirection_dump()
