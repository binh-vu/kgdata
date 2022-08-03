from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.wp2wd import wp2wd
from kgdata.wikipedia.datasets.html_tables import deser_table, html_tables, ser_table
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url
from kgdata.wikipedia.models.linked_html_table import LinkedHTMLTable, WikiLink
import orjson, ujson
from typing import Dict, Iterable, List, Literal, Tuple, TypedDict, Union
from loguru import logger
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, left_outer_join
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle
from table_extractor.table_extractor import HTMLTableExtractor
from table_extractor.models.html_table import HTMLTable, HTMLTableCell
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
        module = import_module(f"kgdata.wikipedia.datasets.{table_dataset_name}")
        table_dataset: Dataset[HTMLTable] = getattr(module, table_dataset_name)()

        tbl2titles = (
            table_dataset.get_rdd()
            .flatMap(extract_title_to_tables)
            .groupByKey()
            .leftOuterJoin(wp2wd(lang).get_rdd())
            .flatMap(lambda x: [(tbl_id, (x[0], x[1][1])) for tbl_id in x[1][0]])
            .groupByKey()
        )

        (
            table_dataset.get_rdd()
            .map(lambda tbl: (tbl.id, tbl))
            .leftOuterJoin(tbl2titles)
            .map(merge_link_to_table)
            .map(ser_linked_tables)
            .coalesce(1024, shuffle=True)
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    ds = Dataset(file_pattern=str(outdir / "*.gz"), deserialize=deser_linked_tables)
    return ds


def linked_relational_tables(lang: str = "en") -> Dataset[LinkedHTMLTable]:
    """Convert Wikipedia links in HTML tables to links in Wikidata using sitelinks

    Args:
        lang: the language of Wikidata
    """
    return linked_tables("relational_tables", lang)


def deser_linked_tables(x: str) -> LinkedHTMLTable:
    return LinkedHTMLTable.from_dict(ujson.loads(x))


def ser_linked_tables(tbl: LinkedHTMLTable) -> str:
    return ujson.dumps(tbl.to_dict(), escape_forward_slashes=False)


def extract_title_to_tables(tbl: HTMLTable) -> List[Tuple[str, str]]:
    """Extract (link, table id) in a table"""
    urls = set()
    for ri, row in enumerate(tbl.rows):
        for ci, cell in enumerate(row.cells):
            urls = urls.union((x.wikipedia_url for x in extract_cell_links(cell)))
    return [(get_title_from_url(url), tbl.id) for url in urls]


def merge_link_to_table(
    x: Tuple[str, Tuple[HTMLTable, Union[None, Iterable[Tuple[str, Union[str, None]]]]]]
) -> LinkedHTMLTable:
    tbl_id, (tbl, title_and_wd) = x
    if title_and_wd is None:
        # happen when the table has no links
        title2wd = {}
    else:
        title2wd = dict(title_and_wd)

    links: Dict[Tuple[int, int], List[WikiLink]] = {}

    if title_and_wd is not None:
        for ri, row in enumerate(tbl.rows):
            for ci, cell in enumerate(row.cells):
                cell_links = extract_cell_links(cell)
                if len(cell_links) > 0:
                    links[ri, ci] = []

                for link in cell_links:
                    title = get_title_from_url(link.wikipedia_url)
                    link.wikidata_id = title2wd[title]
                    links[ri, ci].append(link)

    return LinkedHTMLTable(
        id=tbl_id,
        page_url=tbl.page_url,
        caption=tbl.caption,
        attrs=tbl.attrs,
        context=tbl.context,
        rows=tbl.rows,
        links=links,
    )


def extract_cell_links(cell: HTMLTableCell) -> List[WikiLink]:
    return [
        WikiLink(
            start=el.start, end=el.end, wikipedia_url=el.attrs["href"], wikidata_id=None
        )
        for el in cell.travel_elements_post_order()
        if el.tag == "a" and is_wikipedia_url(el.attrs["href"])
    ]
