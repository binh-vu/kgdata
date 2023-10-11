from importlib import import_module
from typing import Dict, Iterable, List, Literal, Tuple, Union

from kgdata.dataset import Dataset
from kgdata.wikidata.datasets.cross_wiki_mapping import cross_wiki_mapping
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.article_metadata import article_metadata
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url
from kgdata.wikipedia.models.linked_html_table import LinkedHTMLTable, WikiLink
from rsoup.core import Cell, Table


def linked_tables(
    table_dataset_name: Literal["html_tables", "relational_tables"], lang: str = "en"
) -> Dataset[LinkedHTMLTable]:
    """Convert Wikipedia links in HTML tables to links in Wikidata using sitelinks

    Args:
        table_dataset_name: the table dataset to convert
        lang: the language of Wikidata
    """
    cfg = WikipediaDirCfg.get_instance()

    if table_dataset_name == "relational_tables":
        outdir = cfg.linked_relational_tables
    else:
        raise NotImplementedError(table_dataset_name)

    module = import_module(f"kgdata.wikipedia.datasets.{table_dataset_name}")
    table_dataset: Dataset[Table] = getattr(module, table_dataset_name)()
    ds = Dataset(
        file_pattern=outdir / "*.gz",
        deserialize=deser_linked_tables,
        name="linked-relational-tables",
        dependencies=[table_dataset, cross_wiki_mapping(article_metadata())],
    )

    if not ds.has_complete_data():
        module = import_module(f"kgdata.wikipedia.datasets.{table_dataset_name}")
        table_dataset: Dataset[Table] = getattr(module, table_dataset_name)()

        tbl2titles = (
            table_dataset.get_extended_rdd()
            .flatMap(extract_title_to_tables)
            .groupByKey()
            .leftOuterJoin(
                cross_wiki_mapping(article_metadata())
                .get_extended_rdd()
                .map(lambda x: (x.wikipedia_title, x.wikidata_entityid))
            )
            .flatMap(lambda x: [(tbl_id, (x[0], x[1][1])) for tbl_id in x[1][0]])
            .groupByKey()
        )

        (
            table_dataset.get_extended_rdd()
            .map(lambda tbl: (tbl.id, tbl))
            .leftOuterJoin(tbl2titles)
            .map(merge_link_to_table)
            .map(ser_linked_tables)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


def linked_relational_tables(lang: str = "en") -> Dataset[LinkedHTMLTable]:
    """Convert Wikipedia links in HTML tables to links in Wikidata using sitelinks

    Args:
        lang: the language of Wikidata
    """
    return linked_tables("relational_tables", lang)


def deser_linked_tables(x: str) -> LinkedHTMLTable:
    return LinkedHTMLTable.from_json(x)


def ser_linked_tables(tbl: LinkedHTMLTable) -> bytes:
    return tbl.to_json()


def extract_title_to_tables(tbl: Table) -> List[Tuple[str, str]]:
    """Extract (link, table id) in a table"""
    assert is_wikipedia_url(tbl.url), tbl.url

    urls = set()
    urls.add(tbl.url)
    for ri, row in enumerate(tbl.rows):
        for ci, cell in enumerate(row.cells):
            urls = urls.union((x.wikipedia_url for x in extract_cell_links(cell)))
    return [(get_title_from_url(url), tbl.id) for url in urls]


def merge_link_to_table(
    x: Tuple[str, Tuple[Table, Union[None, Iterable[Tuple[str, Union[str, None]]]]]]
) -> LinkedHTMLTable:
    tbl_id, (tbl, title_and_wd) = x
    if title_and_wd is None:
        # happen when the table has no links and url is not linked to any Wikidata entity
        title2wd = {}
    else:
        title2wd = dict(title_and_wd)

    assert is_wikipedia_url(tbl.url), tbl.url
    page_wikidata_id = title2wd.get(get_title_from_url(tbl.url), None)
    links: Dict[Tuple[int, int], List[WikiLink]] = {}

    for ri, ci, cell in tbl.enumerate_cells():
        cell_links = extract_cell_links(cell)
        if len(cell_links) > 0:
            links[ri, ci] = []

        for link in cell_links:
            title = get_title_from_url(link.wikipedia_url)
            link.wikidata_id = title2wd[title]
            links[ri, ci].append(link)

    return LinkedHTMLTable(
        table=tbl,
        links=links,
        page_wikidata_id=page_wikidata_id,
    )


def extract_cell_links(cell: Cell) -> List[WikiLink]:
    links = []

    value = cell.value
    for eid in value.iter_element_id():
        if value.get_element_tag_by_id(eid) == "a":
            href = value.get_element_attr_by_id(eid, "href")
            if href is not None and is_wikipedia_url(href):
                el = value.get_element_by_id(eid)
                link = WikiLink(
                    start=el.start, end=el.end, wikipedia_url=href, wikidata_id=None
                )
                links.append(link)

    return links
