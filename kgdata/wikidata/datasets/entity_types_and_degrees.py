from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entity_all_types import EntityAllTypes, entity_all_types
from kgdata.wikidata.datasets.entity_degrees import EntityDegree, entity_degrees


@dataclass
class EntityTypeAndDegree(Record):
    id: str
    # mapping from type to distance of the correct types
    # zero is the type of that entity, 1 is the parent type, 2 is grand parent, etc.
    types: dict[str, int]
    indegree: int
    outdegree: int

    wikipedia_indegree: Optional[int] = None
    wikipedia_outdegree: Optional[int] = None


@lru_cache()
def entity_types_and_degrees() -> Dataset[EntityTypeAndDegree]:
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        cfg.entity_types_and_degrees / "*.gz",
        deserialize=EntityTypeAndDegree.deser,
        name="entity-types-and-degrees",
        dependencies=[entity_all_types(), entity_degrees()],
    )
    if not ds.has_complete_data():
        (
            entity_all_types()
            .get_extended_rdd()
            .map(lambda e: (e.id, e))
            .join(entity_degrees().get_extended_rdd().map(lambda e: (e.id, e)))
            .map(merge_type_degree)
            .map(EntityTypeAndDegree.ser)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


def merge_type_degree(
    tup: tuple[str, tuple[EntityAllTypes, EntityDegree]]
) -> EntityTypeAndDegree:
    return EntityTypeAndDegree(
        id=tup[0],
        types=tup[1][0].types,
        indegree=tup[1][1].indegree,
        outdegree=tup[1][1].outdegree,
        wikipedia_indegree=tup[1][1].wikipedia_indegree,
        wikipedia_outdegree=tup[1][1].wikipedia_outdegree,
    )
