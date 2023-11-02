from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import orjson
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url
from rsoup.core import Table
from sm.dataset import Context, FullTable
from sm.inputs.column import Column
from sm.inputs.link import EntityId, Link
from sm.inputs.table import ColumnBasedTable
from sm.misc.matrix import Matrix
from sm.namespaces.utils import KGName


@dataclass
class LinkedHTMLTable:
    table: Table
    # mapping from (row, col) => links)
    links: Dict[Tuple[int, int], List[WikiLink]]
    page_wikidata_id: Optional[str] = None

    def to_json(self) -> bytes:
        links = [
            [ri, ci, [l.to_dict() for l in links]]
            for (ri, ci), links in self.links.items()
        ]
        return orjson.dumps([self.table.to_base64(), links, self.page_wikidata_id])

    @staticmethod
    def from_json(s: Union[str, bytes]) -> LinkedHTMLTable:
        tbl, links, page_wikidata_id = orjson.loads(s)
        links = {
            (ri, ci): [WikiLink.from_dict(l) for l in links] for ri, ci, links in links
        }
        return LinkedHTMLTable(
            table=Table.from_base64(tbl),
            page_wikidata_id=page_wikidata_id,
            links=links,
        )

    def to_full_table(self) -> FullTable:
        url = self.table.url
        if is_wikipedia_url(url):
            title = get_title_from_url(url)
        else:
            url = None
            title = None

        table = to_column_based_table(self.table)
        n_headers = self.table.shape()[0] - table.shape()[0]
        links = Matrix.default(table.shape(), list)
        for (ri, ci), lst in self.links.items():
            if ri < n_headers:
                continue
            links[ri - n_headers, ci] = [
                Link(
                    start=l.start,
                    end=l.end,
                    url=l.wikipedia_url,
                    entities=[EntityId(l.wikidata_id, KGName.Wikidata)]
                    if l.wikidata_id is not None
                    else [],
                )
                for l in lst
            ]

        return FullTable(
            table=table,
            context=Context(
                page_title=title,
                page_url=url,
                entities=[EntityId(self.page_wikidata_id, KGName.Wikidata)]
                if self.page_wikidata_id is not None
                else [],
                literals=[],
                content_hierarchy=self.table.context,
            ),
            links=links,
        )


@dataclass
class WikiLink:
    start: int
    end: int  # exclusive
    wikipedia_url: str
    wikidata_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "start": self.start,
            "end": self.end,
            "wikipedia_url": self.wikipedia_url,
            "wikidata_id": self.wikidata_id,
        }

    @staticmethod
    def from_dict(o: dict):
        return WikiLink(**o)


def to_column_based_table(tbl: Table) -> ColumnBasedTable:
    """This code only works for relational table that only the first row is header"""
    from kgdata.wikipedia.datasets.easy_tables import get_n_headers
    from kgdata.wikipedia.datasets.relational_tables import is_relational_table

    assert is_relational_table(tbl) and get_n_headers(tbl) == 1

    nrows, ncols = tbl.shape()
    header = tbl.get_row(0)
    columns = [
        Column(ci, header.get_cell(ci).value.text, values=[]) for ci in range(ncols)
    ]
    for ri in range(1, nrows):
        row = tbl.get_row(ri)
        for ci in range(ncols):
            columns[ci].values.append(row.get_cell(ci).value.text)

    return ColumnBasedTable(tbl.id, columns=columns)
