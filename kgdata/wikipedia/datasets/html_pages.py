import glob
import os
import tarfile
from pathlib import Path
from typing import BinaryIO, Union, cast

import orjson
from kgdata.config import WIKIPEDIA_DIR
from kgdata.spark import get_spark_context
from kgdata.splitter import split_a_file
from pyspark import RDD


def enterprise_html_dumps(
    dump_file: Union[str, Path] = os.path.join(
        WIKIPEDIA_DIR, "dumps/*NS0-*ENTERPRISE-HTML.json.tar.gz"
    ),
    outdir: Union[str, Path] = os.path.join(WIKIPEDIA_DIR, "html_dumps"),
):
    """
    Extract page

    Returns:
        RDD[dict]
    """
    outdir = Path(outdir)

    match_files = glob.glob(str(dump_file))
    if len(match_files) == 0:
        raise Exception("No file found: {}".format(dump_file))
    elif len(match_files) > 1:
        raise Exception("Too many files found: {}".format(dump_file))
    dump_file = match_files[0]

    if not (outdir / "_SUCCESS").exists():
        with tarfile.open(dump_file, "r:*") as archive:
            for file in archive:
                split_a_file(
                    infile=lambda: (
                        file.size,
                        cast(BinaryIO, archive.extractfile(file)),
                    ),
                    outfile=outdir / file.name.split(".", 1)[0] / "part.ndjson.gz",
                    n_writers=8,
                    override=True,
                )
        (outdir / "_SUCCESS").touch()

    sc = get_spark_context()
    return sc.textFile(str(outdir / "*/*.gz")).map(orjson.loads)


if __name__ == "__main__":
    enterprise_html_dumps()
