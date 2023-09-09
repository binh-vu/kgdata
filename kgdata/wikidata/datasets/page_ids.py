from __future__ import annotations

import csv
from functools import lru_cache
from operator import itemgetter
from typing import Optional

from sm.misc.funcs import is_not_null

from kgdata.dataset import Dataset
from kgdata.misc.funcs import split_tab_2
from kgdata.spark import are_records_unique
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entity_ids import is_entity_id
from kgdata.wikidata.datasets.page_dump import page_dump


@lru_cache
def page_ids() -> Dataset[tuple[str, str]]:
    """Get mapping from Wikidata internal page id and Wikidata entity id (possible old id).
    To use the entity id, we should use it with redirections (`entity_redirections`)

    Pages may contain user pages, etc. So we are only keep pages that have entity ids.

    Returns:
        Dataset[tuple[str, str]]
    """
    cfg = WikidataDirCfg.get_instance()

    page_dump_ds = page_dump()
    ds = Dataset(
        file_pattern=cfg.page_ids / "*.gz",
        deserialize=split_tab_2,
        name="page-ids",
        dependencies=[page_dump_ds],
    )

    if not ds.has_complete_data():
        (
            page_dump_ds.get_extended_rdd()
            .flatMap(parse_sql_values)
            .map(extract_id)
            .filter_update_type(is_not_null)
            .map(lambda x: "\t".join(x))
            .save_like_dataset(ds, auto_coalesce=True)
        )

    if not (cfg.page_ids / "_METADATA").exists():
        rdd = ds.get_rdd()
        assert are_records_unique(
            rdd,
            itemgetter(0),
        )
        (cfg.page_ids / "_METADATA").write_text(
            f"""
key.unique: true
n_records: {(rdd.count())}
            """.strip()
        )

    return ds


def extract_id(row: list) -> Optional[tuple[str, str]]:
    # the dumps contain other pages such as user pages, etc.
    page_id, entity_id = row[0], row[2]
    if not is_entity_id(entity_id):
        return None

    assert page_id.isdigit(), page_id
    return page_id, entity_id


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )

    output = []
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    output.append(latest_row)
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            output.append(latest_row)
    return output
