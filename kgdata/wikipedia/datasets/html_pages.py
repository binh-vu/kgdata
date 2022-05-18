import glob
import os
import tarfile
from pathlib import Path
from typing import BinaryIO, Union, cast
from kgdata.wikipedia.config import WPDataDirConfig

import orjson
from kgdata.config import WIKIPEDIA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_file
from pyspark import RDD
from kgdata.dataset import Dataset


def enterprise_html_dumps() -> Dataset[dict]:
    """
    Extract page

    Returns:
        Dataset[dict]
    """
    cfg = WPDataDirConfig.get_instance()

    dump_file = cfg.get_html_dump_file()

    if not does_result_dir_exist(cfg.html_dump):
        with tarfile.open(dump_file, "r:*") as archive:
            for file in archive:
                split_a_file(
                    infile=lambda: (
                        file.size,
                        cast(BinaryIO, archive.extractfile(file)),
                    ),
                    outfile=cfg.html_dump
                    / file.name.split(".", 1)[0]
                    / "part.ndjson.gz",
                    n_writers=8,
                    override=True,
                )
        (cfg.html_dump / "_SUCCESS").touch()

    return Dataset(cfg.html_dump / "*/*.gz", deserialize=orjson.loads)


if __name__ == "__main__":
    enterprise_html_dumps()
