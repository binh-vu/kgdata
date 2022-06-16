import orjson
from loguru import logger
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle
from table_extractor.table_extractor import HTMLTableExtractor
from table_extractor.models.html_table import HTMLTable
import sm.misc as M


def html_tables() -> Dataset[HTMLTable]:
    """Extracting all tables (at the lowest level) and their surrounding context from Wikipedia articles."""

    cfg = WPDataDirConfig.get_instance()

    # resp = (
    #     html_articles()
    #     .get_rdd()
    #     .filter(lambda x: x.page_id == "25966409" or x.page_id == 25966409)
    #     .take(1)
    # )
    # M.serialize_pkl(resp, "/tmp/debug.pkl")
    # return

    if not does_result_dir_exist(cfg.html_tables):
        (
            html_articles()
            .get_rdd()
            .flatMap(extract_tables)
            .saveAsTextFile(
                str(cfg.html_tables),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(file_pattern=cfg.html_tables / "*.gz", deserialize=orjson.loads)


def extract_tables(article: HTMLArticle):
    try:
        tables = HTMLTableExtractor(article.url, article.html, "lxml").extract_tables(
            auto_span=False, auto_pad=False
        )

        return [
            orjson.dumps(
                {
                    "page_id": article.page_id,
                    "url": article.url,
                    "table": tbl.to_dict(),
                }
            )
            for tbl in tables
        ]
    except Exception as e:
        logger.exception(
            "Error while extracting tables from article {}: {}",
            article.page_id,
            article.url,
        )
        return []


if __name__ == "__main__":
    resp = M.deserialize_pkl("/tmp/debug.pkl")
    for article in resp:
        tables = HTMLTableExtractor(article.url, article.html).extract_tables()
        for tbl in tables:
            x = tbl.to_dict()
            orjson.dumps(
                {
                    "page_id": article.page_id,
                    "url": article.url,
                    "table": tbl.to_dict(),
                }
            )

    # print(resp)
