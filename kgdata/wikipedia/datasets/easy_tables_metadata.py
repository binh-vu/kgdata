from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import List, Optional

import orjson
from kgdata.dataset import Dataset
from kgdata.spark import left_outer_join
from kgdata.spark.extended_rdd import ExtendedRDD
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.easy_tables import easy_tables


@dataclass
class TableMetadata:
    id: str
    n_rows: int
    page_types: List[str]


def get_easy_tables_metadata_dataset(with_dep: bool = True) -> Dataset[TableMetadata]:
    cfg = WikipediaDirCfg.get_instance()
    return Dataset(
        file_pattern=cfg.easy_tables_metadata / "*.gz",
        deserialize=deser_easy_tables_metadata,
        name="easy-tables-metadata",
        dependencies=[entity_types(), easy_tables()] if with_dep else [],
    )


@lru_cache()
def easy_tables_metadata() -> Dataset[TableMetadata]:
    ds = get_easy_tables_metadata_dataset()
    if not ds.has_complete_data():
        entity_type_rdd = entity_types().get_extended_rdd()
        table_rdd = (
            easy_tables()
            .get_extended_rdd()
            .map(lambda tbl: (tbl.table.id, tbl.table.n_rows(), tbl.page_wikidata_id))
        )

        new_table_rdd = left_outer_join(
            rdd1=table_rdd.rdd,
            rdd2=entity_type_rdd.rdd,
            rdd1_keyfn=lambda x: x[0],
            rdd1_fk_fn=lambda x: [x[2]] if x[2] is not None else [],
            rdd2_keyfn=lambda x: x[0],
            join_fn=add_page_types,
        )
        ExtendedRDD(new_table_rdd, table_rdd.sig.use(entity_type_rdd.sig)).map(
            lambda tbl: orjson.dumps(asdict(tbl))
        ).save_like_dataset(ds)

    return ds


def deser_easy_tables_metadata(line: str) -> TableMetadata:
    return TableMetadata(**orjson.loads(line))


def add_page_types(
    r1: tuple[str, int, Optional[str]],
    linked_r2: list[tuple[str, Optional[tuple[str, list[str]]]]],
) -> TableMetadata:
    assert len(linked_r2) <= 1
    page_types = []
    if len(linked_r2) > 0 and linked_r2[0][1] is not None:
        page_types = linked_r2[0][1][1]
    return TableMetadata(id=r1[0], n_rows=r1[1], page_types=page_types)
