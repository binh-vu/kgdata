from functools import lru_cache

from loguru import logger
from rsoup.core import ContextExtractor, Table, TableExtractor

from kgdata.dataset import Dataset
from kgdata.spark import are_records_unique
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.models.html_article import HTMLArticle


@lru_cache()
def html_tables() -> Dataset[Table]:
    """Extracting all tables (at the lowest level) and their surrounding context from Wikipedia articles."""

    cfg = WikipediaDirCfg.get_instance()

    ds = Dataset(
        file_pattern=cfg.html_tables / "*.gz",
        deserialize=deser_table,
        # can be json object, or string. it is string when we fail to extract tables from the articles
        prefilter=lambda x: x[0] == "{",
        name="html-tables",
        dependencies=[html_articles()],
    )

    need_double_check = False
    if not ds.has_complete_data():
        (
            html_articles()
            .get_extended_rdd()
            .flatMap(extract_tables)
            .save_like_dataset(ds)
        )
        need_double_check = True

    if need_double_check:
        assert are_records_unique(ds.get_rdd(), lambda tbl: tbl.id)
    return ds


def deser_table(x: str) -> Table:
    return Table.from_json(x)


def ser_table(x: Table) -> str:
    return x.to_json()


def extract_tables(article: HTMLArticle):
    extractor = TableExtractor(
        context_extractor=ContextExtractor(), html_error_forgiveness=True
    )
    try:
        tables = extractor.extract(
            article.url,
            article.html,
            auto_span=True,
            auto_pad=True,
            extract_context=True,
        )
    except Exception as e:
        logger.exception(
            "Error while extracting tables from article {}: {}",
            article.page_id,
            article.url,
        )
        return [article.url]

    # postprocess wikipedia tables
    try:
        for table in tables:
            for cell in table.iter_cells():
                value = cell.value
                for uid in value.iter_element_id():
                    if (
                        value.get_element_tag_by_id(uid) == "a"
                        and value.get_element_attr_by_id(uid, "href") is None
                    ):
                        cls = value.get_element_attr_by_id(uid, "class")
                        if cls is not None and "selflink" in cls:
                            value.set_element_attr_by_id(uid, "href", article.url)
    except:
        logger.exception(
            "Error while posprocessing the extracted tables from article {}: {}",
            article.page_id,
            article.url,
        )
        return [article.url]

    return [tbl.to_json() for tbl in tables]
