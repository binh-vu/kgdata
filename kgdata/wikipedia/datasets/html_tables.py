from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, ensure_unique_records
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.models.html_article import HTMLArticle
from loguru import logger
from rsoup.core import ContextExtractor, Table, TableExtractor


def html_tables() -> Dataset[Table]:
    """Extracting all tables (at the lowest level) and their surrounding context from Wikipedia articles."""

    cfg = WikipediaDirCfg.get_instance()

    need_double_check = False

    if not does_result_dir_exist(cfg.html_tables):
        (
            html_articles()
            .get_rdd()
            .flatMap(extract_tables)
            .coalesce(1024, shuffle=True)
            .saveAsTextFile(
                str(cfg.html_tables),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )
        need_double_check = True

    ds = Dataset(
        file_pattern=cfg.html_tables / "*.gz",
        deserialize=deser_table,
        # can be json object, or string. it is string when we fail to extract tables from the articles
        prefilter=lambda x: x[0] == "{",
    )
    if need_double_check:
        ensure_unique_records(ds.get_rdd(), lambda tbl: tbl.id)
    return ds


def deser_table(x: str) -> Table:
    return Table.from_json(x)


def ser_table(x: Table) -> str:
    return x.to_json()


def extract_tables(article: HTMLArticle):
    extractor = TableExtractor(context_extractor=ContextExtractor())
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
    for table in tables:
        for cell in table.iter_cells():
            value = cell.value
            for uid in value.iter_element_id():
                if (
                    value.get_element_tag_by_id(uid) == "a"
                    and value.get_element_attr_by_id(uid, "href") is None
                ):
                    cls = value.get_element_attr_by_id(uid, "class")
                    assert cls is not None and "selflink" in cls, cls
                    value.set_element_attr_by_id(uid, "href", article.url)

    return [tbl.to_json() for tbl in tables]
