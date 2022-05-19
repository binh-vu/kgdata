from dataclasses import dataclass, field
from operator import itemgetter
from typing import Set, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import copy
import orjson
import pandas as pd

from kgdata.wikidata.models import WDEntity


@dataclass
class Link:
    href: str
    start: int
    end: int
    is_selflink: bool = False

    @property
    def url(self):
        if self.href.startswith("/"):
            return "http://en.wikipedia.org" + self.href
        if self.href.startswith("https://en.wikipedia.org"):
            return self.href.replace("https://", "http://")
        return self.href

    def is_wikipedia_link(self):
        return self.url.startswith("http://en.wikipedia.org")

    def is_wikipedia_article_link(self):
        return self.url.startswith("http://en.wikipedia.org/wiki/")


@dataclass
class ExternalLink:
    __slots__ = ("dbpedia", "qnode", "qnode_id")

    dbpedia: Optional[dict]
    qnode: Optional[WDEntity]
    qnode_id: Optional[str]

    @staticmethod
    def from_dict(o: dict):
        if o["qnode"] is not None:
            o["qnode"] = WDEntity.from_dict(o["qnode"])
        return ExternalLink(**o)


@dataclass
class Cell:
    value: str
    html: str
    links: List[Link] = field(default_factory=list)
    attrs: Dict[str, str] = field(default_factory=dict)
    is_header: bool = False
    rowspan: int = 1
    colspan: int = 1
    original_rowspan: Optional[int] = None
    original_colspan: Optional[int] = None

    @property
    def embedded_html_links(self):
        text = self.value
        embedded_links = []
        for i, link in enumerate(self.links):
            if i == 0:
                embedded_links.append(text[: link.start])
            else:
                assert self.links[i - 1].start <= link.start, "The list is sorted"
                embedded_links.append(text[self.links[i - 1].end : link.start])
            embedded_links.append(
                f'<a href="{link.url}" target="_blank" rel="noopener">'
                + text[link.start : link.end]
                + "</a>"
            )
            if i == len(self.links) - 1:
                embedded_links.append(text[link.end :])
        if len(self.links) == 0:
            embedded_links.append(text)
        return "".join(embedded_links)

    @property
    def this(self):
        """Return its self. Useful if we want to convert the table to list of cells"""
        return self

    @staticmethod
    def from_dict(o: dict):
        o["links"] = [Link(**l) for l in o["links"]]
        return Cell(**o)

    def clone(self):
        """Clone the current cell. This operator will be much cheaper than the deepcopy operator"""
        return Cell(
            value=self.value,
            html=self.html,
            links=copy.copy(self.links),
            attrs=copy.copy(self.attrs),
            is_header=self.is_header,
            rowspan=self.rowspan,
            colspan=self.colspan,
            original_rowspan=self.original_rowspan,
            original_colspan=self.original_colspan,
        )


@dataclass
class Row:
    cells: List[Cell]
    attrs: Dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_dict(o: dict):
        o["cells"] = [Cell.from_dict(c) for c in o["cells"]]
        return Row(**o)

    def __getitem__(self, index):
        return self.cells[index]


class OverlapSpanException(Exception):
    """Indicating the table has cell rowspan and cell colspan overlaps"""

    pass


class InvalidColumnSpanException(Exception):
    """Indicating that the column span is not used in a standard way. In particular, the total of columns' span is beyond the maximum number of columns is considered
    to be non standard with one exception that only the last column spans more than the maximum number of columns
    """

    pass


@dataclass
class Table:
    id: str
    pageURI: str
    rows: List[Row]
    caption: Optional[str] = None
    attrs: Dict[str, str] = field(default_factory=dict)
    classes: Set[str] = field(default_factory=set)
    is_spanned: bool = False
    external_links: Optional[Dict[str, ExternalLink]] = None

    @staticmethod
    def from_dict(o: dict):
        o["classes"] = set(o["classes"])
        o["rows"] = [Row.from_dict(r) for r in o["rows"]]
        if o["external_links"] is not None:
            for k, v in o["external_links"].items():
                o["external_links"][k] = (
                    ExternalLink.from_dict(v) if v is not None else None
                )

        return Table(**o)

    @staticmethod
    def deser_str(text):
        return Table.from_dict(orjson.loads(text))

    @property
    def wikipediaURL(self):
        """Return a wikipedia URL from dbpedia URI"""
        return urlparse(self.pageURI).path.replace(
            "/resource/", "https://en.wikipedia.org/wiki/"
        )

    def ser_bytes(self):
        return orjson.dumps(self, option=orjson.OPT_SERIALIZE_DATACLASS, default=list)

    def pad(self) -> "Table":
        """Pad the irregular table (missing cells) to make it become regular table.

        This function only return new table when it's padded
        """
        if not self.is_spanned:
            raise Exception("You should only pad the table after it's spanned")

        if self.is_regular_table():
            return self

        max_ncols = max(len(r.cells) for r in self.rows)
        default_cell = Cell(value="", html="", original_rowspan=1, original_colspan=1)

        rows = []
        for r in self.rows:
            row = Row(cells=[c.clone() for c in r.cells], attrs=copy.copy(r.attrs))
            while len(row.cells) < max_ncols:
                row.cells.append(default_cell.clone())
            rows.append(row)

        return Table(
            id=self.id,
            pageURI=self.pageURI,
            classes=copy.copy(self.classes),
            rows=rows,
            caption=self.caption,
            attrs=copy.copy(self.attrs),
            is_spanned=True,
            external_links=copy.copy(self.external_links),
        )

    def span(self) -> "Table":
        """Span the table by copying values to merged field"""
        pi = 0
        data = []
        pending_ops = {}

        # >>> begin find the max #cols
        # calculate the number of columns as some people may actually set unrealistic colspan as they are lazy..
        # I try to make its behaviour as much closer to the browser as possible.
        # one thing I notice that to find the correct value of colspan, they takes into account the #cells of rows below the current row
        # so we may have to iterate several times
        cols = [0 for _ in range(len(self.rows))]
        for i, row in enumerate(self.rows):
            cols[i] += len(row.cells)
            for cell in row.cells:
                if cell.rowspan > 1:
                    for j in range(1, cell.rowspan):
                        if i + j < len(cols):
                            cols[i + j] += 1

        _row_index, max_ncols = max(enumerate(cols), key=itemgetter(1))
        # sometimes they do show an extra cell for over-colspan row, but it's not consistent or at least not easy for me to find the rule
        # so I decide to not handle that. Hope that we don't have many tables like that.
        # >>> finish find the max #cols

        for row in self.rows:
            new_row = []
            pj = 0
            for cell_index, cell in enumerate(row.cells):
                cell = cell.clone()
                cell.original_colspan = cell.colspan
                cell.original_rowspan = cell.rowspan
                cell.colspan = 1
                cell.rowspan = 1

                # adding cell from the top
                while (pi, pj) in pending_ops:
                    new_row.append(pending_ops[pi, pj].clone())
                    pending_ops.pop((pi, pj))
                    pj += 1

                # now add cell and expand the column
                for _ in range(cell.original_colspan):
                    if (pi, pj) in pending_ops:
                        # exception, overlapping between colspan and rowspan
                        raise OverlapSpanException()
                    new_row.append(cell.clone())
                    for ioffset in range(1, cell.original_rowspan):
                        # no need for this if below
                        # if (pi+ioffset, pj) in pending_ops:
                        #     raise OverlapSpanException()
                        pending_ops[pi + ioffset, pj] = cell
                    pj += 1

                    if pj >= max_ncols:
                        # our algorithm cannot handle the case where people are bullying the colspan system, and only can handle the case
                        # where the span that goes beyond the maximum number of columns is in the last column.
                        if cell_index != len(row.cells) - 1:
                            raise InvalidColumnSpanException()
                        else:
                            break

            # add more cells from the top since we reach the end
            while (pi, pj) in pending_ops and pj < max_ncols:
                new_row.append(pending_ops[pi, pj].clone())
                pending_ops.pop((pi, pj))
                pj += 1

            data.append(Row(cells=new_row, attrs=copy.copy(row.attrs)))
            pi += 1

        # len(pending_ops) may > 0, but fortunately, it doesn't matter as the browser also does not render that extra empty lines
        return Table(
            id=self.id,
            pageURI=self.pageURI,
            classes=copy.copy(self.classes),
            rows=data,
            caption=self.caption,
            attrs=copy.copy(self.attrs),
            is_spanned=True,
            external_links=copy.copy(self.external_links),
        )

    def is_regular_table(self) -> bool:
        ncols = len(self.rows[0].cells)
        return all(len(self.rows[i].cells) == ncols for i in range(1, len(self.rows)))

    def get_shape(self) -> Tuple[int, int]:
        """Get shape of the table.

        Returns
        -------
        Tuple[int, int]
            A 2-tuples of rows' number and cols' number

        Raises
        ------
        Exception
            When this is not a regular table (not 2D)
        """
        if not self.is_regular_table():
            raise Exception("Cannot get shape of an irregular table")
        return len(self.rows), len(self.rows[0].cells)

    def to_list(self, value_field: str = "value") -> list:
        """Convert the table into a list

        Parameters
        ----------
        value_field: str, optional
            cell's field that we want to use as a value, default is `value`. Change to html or inner html to see the html

        Returns
        -------
        list
        """
        if not self.is_spanned:
            return self.span().to_list()
        return [[getattr(c, value_field) for c in row.cells] for row in self.rows]

    def to_df(
        self, use_header_when_possible: bool = True, value_field: str = "value"
    ) -> pd.DataFrame:
        """Convert table to dataframe. Auto-span the row by default. This will throw error
        if the current table is not a regular table.

        Parameters
        ----------
        use_header_when_possible : bool, optional
            if the first row is all header cells, we use it as the header. Note that other header cells
            which aren't on the first row are going to be treated as data cells
        value_field: str, optional
            cell's field that we want to use as a value, default is `value`. Change to html or inner html to see the html
        Returns
        -------
        pd.DataFrame
            Data frame
        """
        if not self.is_spanned:
            return self.span().to_df(use_header_when_possible)

        data = [[getattr(c, value_field) for c in r.cells] for r in self.rows]

        if use_header_when_possible:
            if all(c.is_header for c in self.rows[0].cells):
                return pd.DataFrame(data[1:], columns=data[0])
        return pd.DataFrame(data)

    def __getitem__(self, index):
        return self.rows[index]
