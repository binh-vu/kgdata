from typing import Callable, List
from functools import partial
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.datasets.linked_relational_tables import (
    linked_relational_tables,
    ser_linked_tables,
    deser_linked_tables,
)
from kgdata.wikipedia.models.linked_html_table import LinkedHTMLTable


def easy_tables() -> Dataset[LinkedHTMLTable]:
    """Tables that can be labeled automatically easily.
    The table is easy or not is determined by :py:func:`kgdata.wikipedia.easy_table.is_easy_table`

    """
    cfg = WPDataDirConfig.get_instance()

    # step 1: generate stats of which tables passed which tests

    # step 2: filter the tables
    if not does_result_dir_exist(cfg.easy_tables):
        tests = [
            EasyTests.min_rows,
            EasyTests.min_links_all_columns,
            EasyTests.min_link_coverage_all_columns,
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
            .get_rdd()
            .filter(partial(is_easy_table, tests=tests))
            .map(ser_linked_tables)
            .coalesce(192, shuffle=True)
            .saveAsTextFile(
                str(cfg.easy_tables),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        file_pattern=cfg.easy_tables / "*.gz", deserialize=deser_linked_tables
    )


def is_easy_table(
    tbl: LinkedHTMLTable, tests: List[Callable[[LinkedHTMLTable], bool]]
) -> bool:
    """Determine if a table is easy or not.

    Args:
        tbl: Input table.
        tests: List of tests. Each test is a function that takes a table and returns a boolean.
    """
    return all(test(tbl) for test in tests)


def get_n_headers(tbl: LinkedHTMLTable) -> int:
    n_headers = 0
    try:
        for row in tbl.table.iter_rows():
            if not row.get_cell(0).is_header:
                break
            n_headers += 1
    except KeyError as e:
        raise KeyError(str(e) + " " + str((">>>", tbl.table.id, tbl.table.url)))
    return n_headers


class EasyTests:
    MIN_ROWS = 10
    MIN_FREQ_LINKS = 0.7
    MIN_LINK_SURFACE = 0.9
    MIN_EXISTING_LINKS = 0.8

    @staticmethod
    def min_rows(tbl: LinkedHTMLTable) -> bool:
        """Determine if a table has at least min_rows rows.

        Args:
            tbl: Input table.
            min_rows: Minimum number of rows.
        """
        # minus one for the header
        n_headers = get_n_headers(tbl)
        return tbl.table.n_rows() - n_headers >= EasyTests.MIN_ROWS

    @staticmethod
    def min_link_coverage_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        if nrows == 0:
            return False

        for ci in range(ncols):
            percent_surface = (
                sum(
                    sum((link.end - link.start) for link in tbl.links.get((ri, ci), []))
                    / max(0.1, tbl.table.get_cell(ri, ci).value.len())
                    for ri in range(1, nrows + 1)
                )
                / nrows
            )
            if percent_surface > 0 and percent_surface < EasyTests.MIN_LINK_SURFACE:
                return False
        return True

    @staticmethod
    def min_links_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        if nrows == 0:
            return False

        for ci in range(ncols):
            nlinks = sum(
                int(len(tbl.links.get((ri, ci), [])) > 0) for ri in range(1, nrows + 1)
            )
            if nlinks > 0 and nlinks / nrows < EasyTests.MIN_FREQ_LINKS:
                return False
        return True

    @staticmethod
    def single_links_all_columns(tbl: LinkedHTMLTable) -> bool:
        n_headers = get_n_headers(tbl)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        for ci in range(ncols):
            count = 0
            total = 0
            for ri in range(1, nrows + 1):
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
        n_headers = get_n_headers(tbl)
        nrows, ncols = tbl.table.shape()
        nrows -= n_headers

        for ci in range(ncols):
            n_existing_links = 0
            total = 0
            for ri in range(1, nrows + 1):
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
