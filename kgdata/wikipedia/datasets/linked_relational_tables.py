from kgdata.wikidata.datasets.wp2wd import wp2wd
from kgdata.wikipedia.datasets.html_tables import deser_table, html_tables, ser_table
from kgdata.wikipedia.models.linked_html_table import LinkedHTMLTable
import orjson
from typing import Literal, TypedDict, Union
from loguru import logger
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle
from table_extractor.table_extractor import HTMLTableExtractor
from table_extractor.models.html_table import HTMLTable
import sm.misc as M
from importlib import import_module


def linked_tables(
    table_dataset_name: Literal["html_tables", "relational_tables"], lang: str = "en"
) -> Dataset[LinkedHTMLTable]:
    """Convert Wikipedia links in HTML tables to links in Wikidata using sitelinks

    Args:
        table_dataset_name: the table dataset to convert
        lang: the language of Wikidata
    """
    cfg = WPDataDirConfig.get_instance()

    if table_dataset_name == "relational_tables":
        outdir = cfg.linked_relational_tables
    else:
        raise NotImplementedError(table_dataset_name)

    if not does_result_dir_exist(outdir):
        wp2wd_dataset = wp2wd(lang)

        module = import_module(f"kgdata.wikipedia.datasets.{table_dataset_name}")
        table_dataset: Dataset[HTMLTable] = getattr(module, table_dataset_name)()

        table_dataset.get_rdd().map(extract_table_links).groupByKey()

    return Dataset(file_pattern=str(outdir / "*.gz"), deserialize=deser_linked_tables)


def deser_linked_tables(x: Union[str, bytes]) -> HTMLTable:
    return HTMLTable.from_dict(orjson.loads(x))


def extract_table_links(tbl: HTMLTable):
    pass
