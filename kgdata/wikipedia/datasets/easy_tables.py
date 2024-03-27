import re
from functools import partial
from typing import Callable, List

from kgdata.dataset import Dataset
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.linked_relational_tables import (
    deser_linked_tables,
    linked_relational_tables,
    ser_linked_tables,
)
from kgdata.wikipedia.models.linked_html_table import LinkedHTMLTable
from rsoup.core import Table


def get_easy_tables_dataset(with_dep: bool = True) -> Dataset[LinkedHTMLTable]:
    cfg = WikipediaDirCfg.get_instance()
    return Dataset(
        file_pattern=cfg.easy_tables / "*.gz",
        deserialize=deser_linked_tables,
        name="easy-tables",
        dependencies=[linked_relational_tables()] if with_dep else [],
    )


def easy_tables() -> Dataset[LinkedHTMLTable]:
    """Tables that can be labeled automatically easily.
    The table is easy or not is determined by :py:func:`kgdata.wikipedia.easy_table.is_easy_table`.
    """
    ds = get_easy_tables_dataset()

    # step 1: generate stats of which tables passed which tests

    # step 2: filter the tables
    if not ds.has_complete_data():
        tests = [
            EasyTests.only_first_row_header,
            EasyTests.no_spanning_header_columns,
            EasyTests.min_rows,
            EasyTests.has_link,
            EasyTests.min_links_all_columns,
            EasyTests.min_link_coverage_all_columns,
            EasyTests.single_links_all_columns,
            EasyTests.min_existing_links_all_columns,
        ]

        # print(
        #     (
        #         linked_relational_tables()
        #         .get_rdd()
        #         .filter(partial(is_easy_table, tests=tests))
        #         .count()
        #     )
        # )
        (
            linked_relational_tables()
            .get_extended_rdd()
            .filter(partial(is_easy_table, tests=tests))
            .map(ser_linked_tables)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def is_easy_table(
    tbl: LinkedHTMLTable, tests: List[Callable[[LinkedHTMLTable], bool]]
) -> bool:
    """Determine if a table is easy or not.

    Args:
        tbl: Input table.
        tests: List of tests. Each test is a function that takes a table and returns a boolean.
    """
    return all(test(tbl) for test in tests)


def get_n_headers(tbl: Table) -> int:
    n_headers = 0
    try:
        for row in tbl.iter_rows():
            if not row.get_cell(0).is_header:
                break
            n_headers += 1
    except KeyError as e:
        raise KeyError(str(e) + " " + str((">>>", tbl.id, tbl.url)))
    return n_headers


class EasyTests:
    MIN_ROWS = 10
    MIN_FREQ_LINKS = 0.7
    MIN_LINK_SURFACE = 0.9
    MIN_EXISTING_LINKS = 0.8

    @staticmethod
    def only_first_row_header(tbl: LinkedHTMLTable) -> bool:
        """Determine if a table has only one row of headers."""
        n_headers = get_n_headers(tbl.table)
        return n_headers == 1

    @staticmethod
    def no_spanning_header_columns(tbl: LinkedHTMLTable) -> bool:
        """Determine if a table has no spanning columns."""
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()

        for ri in range(n_headers):
            row = tbl.table.get_row(ri)
            for ci in range(ncols):
                header = row.get_cell(ci)
                thels = []
                for eid in header.value.iter_element_id():
                    tag = header.value.get_element_tag_by_id(eid)
                    if tag == "th":
                        thels.append(eid)

                if len(thels) == 0:
                    # happens when the table is irregular table (e.g., missing columns) and additional columns are added so we can't find the header
                    assert header.value.text == ""
                    return False

                assert (
                    len(thels) == 1
                ), f"Table {tbl.table.id} must have exactly one <th> element."
                thel = thels[0]
                colspan = header.value.get_element_attr_by_id(thel, "colspan")
                # html forgiveness is enable, so we accept value such as 100%, etc, and only
                # use the number at the beginning just like the browser.
                if (
                    colspan is not None
                    and colspan != ""
                    and (m := re.search(r"\d+", colspan)) is not None
                    and int(m.group(0)) > 1
                ):
                    return False
        return True

    @staticmethod
    def has_link(tbl: LinkedHTMLTable) -> bool:
        """Determine if a table has at least one link."""
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()

        for ri in range(n_headers, nrows):
            for ci in range(ncols):
                if (ri, ci) in tbl.links and len(tbl.links[ri, ci]) > 0:
                    return True
        return False

    @staticmethod
    def min_rows(tbl: LinkedHTMLTable) -> bool:
        """Determine if a table has at least min_rows rows.

        Args:
            tbl: Input table.
            min_rows: Minimum number of rows.
        """
        # minus one for the header
        n_headers = get_n_headers(tbl.table)
        return tbl.table.n_rows() - n_headers >= EasyTests.MIN_ROWS

    @staticmethod
    def min_link_coverage_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        if nrows == 0:
            return False

        for ci in range(ncols):
            nlinks = sum(
                int(len(tbl.links.get((ri, ci), [])) > 0)
                for ri in range(n_headers, nrows + n_headers)
            )
            if nlinks == 0:
                continue
            percent_surface = (
                sum(
                    sum((link.end - link.start) for link in tbl.links.get((ri, ci), []))
                    / max(0.1, tbl.table.get_cell(ri, ci).value.len())
                    for ri in range(n_headers, nrows + n_headers)
                )
                / nrows
            )
            if percent_surface < EasyTests.MIN_LINK_SURFACE:
                return False
        return True

    @staticmethod
    def min_links_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        if nrows == 0:
            return False

        for ci in range(ncols):
            nlinks = sum(
                int(len(tbl.links.get((ri, ci), [])) > 0)
                for ri in range(n_headers, nrows + n_headers)
            )
            if nlinks > 0 and nlinks / nrows < EasyTests.MIN_FREQ_LINKS:
                return False
        return True

    @staticmethod
    def single_links_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        for ci in range(ncols):
            count = 0
            total = 0
            for ri in range(n_headers, nrows + n_headers):
                if (ri, ci) not in tbl.links:
                    continue
                nonempty_links = [
                    link for link in tbl.links[ri, ci] if link.end > link.start
                ]
                if len(nonempty_links) > 0:
                    count += int(len(nonempty_links) == 1)
                    total += 1
            if count != total:
                return False
        return True

    @staticmethod
    def min_existing_links_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl.table)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        for ci in range(ncols):
            n_existing_links = 0
            total = 0
            for ri in range(n_headers, nrows + n_headers):
                if (ri, ci) not in tbl.links:
                    continue
                nonempty_links = [
                    link for link in tbl.links[ri, ci] if link.end > link.start
                ]
                if len(nonempty_links) > 0:
                    n_existing_links += sum(
                        int(link.wikidata_id is not None) for link in nonempty_links
                    ) / len(nonempty_links)
                    total += 1

            if total > 0 and n_existing_links / total < EasyTests.MIN_EXISTING_LINKS:
                return False

        return True
