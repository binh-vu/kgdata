import glob
import os
import tarfile
from pathlib import Path
from typing import BinaryIO, Union, cast
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle

import orjson
from kgdata.config import WIKIPEDIA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_file
from kgdata.dataset import Dataset


def html_articles() -> Dataset[HTMLArticle]:
    """
    Extract HTML page

    Returns:
        Dataset[HTMLArticle]
    """
    cfg = WPDataDirConfig.get_instance()

    dump_file = cfg.get_html_article_file()

    if not does_result_dir_exist(cfg.html_articles):
        with tarfile.open(dump_file, "r:*") as archive:
            for file in archive:
                split_a_file(
                    infile=lambda: (
                        file.size,
                        cast(BinaryIO, archive.extractfile(file)),
                    ),
                    outfile=cfg.html_articles
                    / file.name.split(".", 1)[0]
                    / "part.ndjson.gz",
                    n_writers=8,
                    override=True,
                    n_records_per_file=3000,
                )
        (cfg.html_articles / "_SUCCESS").touch()

    return Dataset(cfg.html_articles / "*/*.gz", deserialize=deser_html_articles)


def deser_html_articles(line: Union[str, bytes]) -> HTMLArticle:
    return HTMLArticle.from_dump_dict(orjson.loads(line))


def ser_html_articles(article: HTMLArticle) -> bytes:
    return orjson.dumps(article.to_dict())
