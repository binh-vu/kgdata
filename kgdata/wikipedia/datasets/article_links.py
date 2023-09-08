from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache, partial
from urllib.parse import urljoin, urlparse

from rsoup.core import Document, RichText, RichTextConfig

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.models.html_article import HTMLArticle


@dataclass
class Href:
    url: str
    text: RichText

    @staticmethod
    def from_dict(obj: dict):
        return Href(obj["url"], RichText.from_dict(obj["text"]))

    def to_dict(self):
        return {"url": self.url, "text": self.text.to_dict()}


@dataclass
class ArticleLinks:
    # the following properties: page_id, name, url are copied from html articles
    page_id: int
    name: str
    url: str
    targets: list[Href]

    @staticmethod
    def from_dict(obj):
        return ArticleLinks(
            obj["page_id"],
            obj["name"],
            obj["url"],
            [Href.from_dict(x) for x in obj["targets"]],
        )

    def to_dict(self):
        return {
            "page_id": self.page_id,
            "name": self.name,
            "url": self.url,
            "targets": [x.to_dict() for x in self.targets],
        }


@lru_cache()
def article_links() -> Dataset[ArticleLinks]:
    cfg = WikipediaDirCfg.get_instance()
    ds = Dataset(
        file_pattern=cfg.article_links / "*.gz",
        deserialize=partial(deser_from_dict, ArticleLinks),
        name="article-links",
        dependencies=[html_articles()],
    )

    if not ds.has_complete_data():
        (
            html_articles()
            .get_extended_rdd()
            .map(extract_links)
            .map(ser_to_dict)
            .save_like_dataset(ds)
        )

    return ds


def extract_links(article: HTMLArticle) -> ArticleLinks:
    doc = Document(article.url, article.html)
    rich_text_cfg = RichTextConfig(
        ignored_tags=["script", "style", "noscript", "table"],
        only_inline_tags=True,
        discard_tags=["div"],
        keep_tags=["ol", "ul", "li"],
    )

    targets = []
    for el in doc.select("a"):
        url = el.attr("href")
        if url is None:
            cls = el.attr("class")
            if "selflink" not in cls:
                continue
            url = article.url

        if is_relative_url(url):
            url = urljoin(article.url, url)
        targets.append(Href(url=url, text=el.get_rich_text(rich_text_cfg)))

    return ArticleLinks(
        page_id=article.page_id, name=article.name, url=article.url, targets=targets
    )


def is_relative_url(url: str) -> bool:
    return urlparse(url).netloc == ""
