from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.models.html_article import (
    AdditionalEntity,
    HTMLArticle,
    NameAndURL,
)


@dataclass
class ArticleMetadata(Record):
    # page title, help get access the article by replacing space with underscore
    name: str

    # page id, can help access the article by /?curid=id
    page_id: int

    # utc string specified the modification time of the article
    date_modified: str

    # url of the article
    url: str

    # language of the page e.g., en
    lang: str

    # wikidata entity associated with the page
    wdentity: Optional[str]
    # additional entities associated with the page
    additional_entities: list[AdditionalEntity]

    # part of which wikipedia, e.g., enwiki
    is_part_of: str

    # list of wikipedia categories
    categories: list[NameAndURL]

    # list of wikipedia pages that redirect to this page
    redirects: list[NameAndURL]


@lru_cache()
def article_metadata() -> Dataset[ArticleMetadata]:
    cfg = WikipediaDirCfg.get_instance()
    ds = Dataset(
        cfg.article_metadata / "*.gz",
        deserialize=ArticleMetadata.deser,
        name="article-metadata",
        dependencies=[html_articles()],
    )

    if not ds.has_complete_data():
        (
            html_articles()
            .get_extended_rdd()
            .map(extract_metadata)
            .map(ArticleMetadata.ser)
            .save_like_dataset(ds)
        )

    return ds


def extract_metadata(article: HTMLArticle) -> ArticleMetadata:
    return ArticleMetadata(
        name=article.name,
        page_id=article.page_id,
        date_modified=article.date_modified,
        url=article.url,
        lang=article.lang,
        wdentity=article.wdentity,
        additional_entities=article.additional_entities,
        is_part_of=article.is_part_of,
        categories=article.categories,
        redirects=article.redirects,
    )
