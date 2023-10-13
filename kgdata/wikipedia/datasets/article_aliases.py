from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache, partial

import orjson
from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.article_links import ArticleLinks, article_links


@dataclass
class ArticleAliases:
    url: str  # url of html articles
    aliases: dict[str, int]  # mention and count

    @staticmethod
    def from_dict(obj):
        return ArticleAliases(
            obj["url"],
            obj["aliases"],
        )

    def to_dict(self):
        return {
            "url": self.url,
            "aliases": self.aliases,
        }


@lru_cache()
def article_aliases() -> Dataset[ArticleAliases]:
    """Extract a mapping from article urls to set of names that were used to refer to that article."""

    cfg = WikipediaDirCfg.get_instance()

    ds = Dataset(
        file_pattern=cfg.article_aliases / "*.gz",
        deserialize=partial(deser_from_dict, ArticleAliases),
        name="article-aliases",
        dependencies=[article_links()],
    )

    if not ds.has_complete_data():
        (
            article_links()
            .get_extended_rdd()
            .flatMap(extract_aliases)
            .map(lambda a: (a.url, a))
            .reduceByKey(merge_aliases)
            .map(lambda tup: orjson.dumps(tup[1].to_dict()))
            .save_like_dataset(ds)
        )

    return ds


def extract_aliases(article: ArticleLinks) -> list[ArticleAliases]:
    url2aliases = {}
    for link in article.targets:
        if link.url not in url2aliases:
            url2aliases[link.url] = ArticleAliases(link.url, {})
        mention = link.text.text
        aliases = url2aliases[link.url].aliases
        if mention not in aliases:
            aliases[mention] = 1
        else:
            aliases[mention] += 1

    return list(url2aliases.values())


def merge_aliases(alias1: ArticleAliases, alias2: ArticleAliases) -> ArticleAliases:
    aliases = alias1.aliases.copy()
    for mention, count in alias2.aliases.items():
        if mention not in aliases:
            aliases[mention] = count
        else:
            aliases[mention] += count
    return ArticleAliases(url=alias1.url, aliases=aliases)
