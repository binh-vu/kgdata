from __future__ import annotations

from dataclasses import dataclass
from operator import add
from typing import Optional
from urllib.parse import urlparse

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
    ds = Dataset(cfg.article_degrees / "*.gz", deserialize=ArticleDegree.deser)

    if not does_result_dir_exist(cfg.article_degrees):
        rdd = article_links().get_extended_rdd()

        indegree = rdd.flatMap(
            lambda a: ((target.url, 1) for target in a.targets)
        ).reduceByKey(add)

        (
            rdd.map(
                lambda a: (
                    a.url,
                    sum((int(is_article_url(target.url)) for target in a.targets)),
                )
            )
            .leftOuterJoin(indegree)
            .map(merge_degree)
            .map(ArticleDegree.ser)
            .save_as_dataset(
                cfg.article_degrees,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
                name="article-degrees",
            )
        )

    return ds


def merge_degree(tup: tuple[str, tuple[int, Optional[int]]]) -> ArticleDegree:
    url, (outdegree, indegree) = tup
    return ArticleDegree(url, indegree if indegree is not None else 0, outdegree)


def is_article_url(url: str) -> bool:
    parsed_url = urlparse(url)
    return parsed_url.netloc.endswith("wikipedia.org") and parsed_url.path.startswith(
        "/wiki/"
    )
