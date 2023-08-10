from __future__ import annotations

from dataclasses import dataclass
from operator import add
from typing import Optional

from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.article_links import article_links


@dataclass
class ArticleDegree(Record):
    url: str
    indegree: int
    outdegree: int


def article_degrees(lang: str = "en") -> Dataset[ArticleDegree]:
    """Computes the indegree and outdegree of all articles in the wikipedia
    corpus. The result is a dictionary mapping from article id to indegree and
    outdegree.
    """

    cfg = WikipediaDirCfg.get_instance()

    if not does_result_dir_exist(cfg.article_degrees):
        ds = article_links().get_rdd()

        indegree = ds.flatMap(
            lambda a: ((target.url, 1) for target in a.targets)
        ).reduceByKey(add)

        (
            ds.map(lambda a: (a.url, len(a.targets)))
            .leftOuterJoin(indegree)
            .map(merge_degree)
            .map(ArticleDegree.ser)
            .saveAsTextFile(
                str(cfg.article_degrees),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.article_degrees / "*.gz", deserialize=ArticleDegree.deser)


def merge_degree(tup: tuple[str, tuple[int, Optional[int]]]) -> ArticleDegree:
    url, (outdegree, indegree) = tup
    return ArticleDegree(url, indegree if indegree is not None else 0, outdegree)
