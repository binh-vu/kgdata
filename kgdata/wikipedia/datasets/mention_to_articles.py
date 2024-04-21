from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from operator import add

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.article_links import ArticleLinks, article_links
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url


@dataclass
class MentionToArticles:
    mention: str
    # article title to two frequencies:
    # 0: global --- like concatenating all documents into a single document and count
    # 1: adjusted --- a mention may appear multiple times in a single document but we only count as 1
    target_articles: dict[str, tuple[int, int]]

    @staticmethod
    def from_dict(obj: dict) -> MentionToArticles:
        return MentionToArticles(obj["mention"], obj["target_articles"])

    def to_dict(self) -> dict:
        return {"mention": self.mention, "target_articles": self.target_articles}


def mention_to_articles() -> Dataset[MentionToArticles]:
    cfg = WikipediaDirCfg.get_instance()
    ds = Dataset(
        file_pattern=cfg.mention_to_articles / "*.gz",
        deserialize=partial(deser_from_dict, MentionToArticles),
        name="mention-to-articles",
        dependencies=[article_links()],
    )

    if not ds.has_complete_data():
        print(
            article_links()
            .get_extended_rdd()
            .flatMap(extract_mention_to_articles)
            .take(10)
        )
        (
            article_links()
            .get_extended_rdd()
            .flatMap(extract_mention_to_articles)
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
            .map(lambda x: (x[0][0], (x[0][1], x[1])))
            .groupByKey()
            .map(lambda x: MentionToArticles(x[0], dict(x[1])))
            .map(ser_to_dict)
            .save_like_dataset(ds)
        )

    return ds


def extract_mention_to_articles(
    article_link: ArticleLinks,
) -> list[tuple[tuple[str, str], tuple[int, int]]]:
    output = defaultdict(int)
    for target in article_link.targets:
        if not is_wikipedia_url(target.url):
            continue
        article = get_title_from_url(target.url)
        if article == "":
            continue

        output[(target.text.text, article)] += 1

    return [(k, (v, 1)) for k, v in output.items()]
