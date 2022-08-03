from urllib.parse import urlparse
import orjson
from typing import TypedDict, Union
from loguru import logger
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle
from table_extractor.table_extractor import HTMLTableExtractor
from table_extractor.models.html_table import HTMLTable
import sm.misc as M
import ujson


def html_tables() -> Dataset[HTMLTable]:
    """Extracting all tables (at the lowest level) and their surrounding context from Wikipedia articles."""

    cfg = WPDataDirConfig.get_instance()

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

    return Dataset(
        file_pattern=cfg.html_tables / "*.gz",
        deserialize=deser_table,
        # can be json object, or string. it is string when we fail to extract tables from the articles
        prefilter=lambda x: x[0] == "{",
    )


def deser_table(x: str) -> HTMLTable:
    return HTMLTable.from_dict(ujson.loads(x))


def ser_table(x: HTMLTable) -> str:
    return ujson.dumps(x.to_dict(), escape_forward_slashes=False)


def extract_tables(article: HTMLArticle):
    try:
        tables = HTMLTableExtractor(article.url, article.html, "lxml").extract_tables(
            auto_span=True, auto_pad=True, extract_context=False
        )
    except Exception as e:
        logger.exception(
            "Error while extracting tables from article {}: {}",
            article.page_id,
            article.url,
        )
        return [article.url]

    # fix Wikipedia relative links to absolute links
    # html static articles store the relative links strangely
    # ./Kendal => https://en.wikipedia.org/wiki/Kendal
    # while it should be /wiki/Kendal.
    # if the relative links are interpreted correctly, it will be https://en.wikipedia.org/wiki/<current_page>/Kendal
    parsed_resp = urlparse(article.url)
    domain = f"{parsed_resp.scheme}://{parsed_resp.netloc}/wiki"

    for table in tables:
        for row in table.rows:
            for cell in row.cells:
                for el in cell.travel_elements_post_order():
                    if el.tag != "a" or "href" not in el.attrs:
                        continue

                    href = el.attrs["href"]
                    if href.startswith("./"):
                        # assert el.attrs.get("rel", [""])[0] == "mw:WikiLink", (
                        #     table.page_url,
                        #     el,
                        # )
                        el.attrs["href"] = domain + href[1:]

    return [ujson.dumps(tbl.to_dict()) for tbl in tables]


if __name__ == "__main__":
    resp = M.deserialize_pkl("/tmp/debug.pkl")
    for article in resp:
        tables = HTMLTableExtractor(article.url, article.html).extract_tables()
        for tbl in tables:
            x = tbl.to_dict()
            orjson.dumps(tbl.to_dict())

    print(resp[0].page_id)
