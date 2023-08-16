from __future__ import annotations

import math
from dataclasses import dataclass
from functools import partial
from random import randrange
from typing import Iterable, Optional

import orjson
from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.models.ont_class import OntologyClass
from kgdata.spark import are_records_unique, does_result_dir_exist, get_spark_context
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.entity_all_types import EntityAllTypes, entity_all_types
from kgdata.wikidata.datasets.entity_degrees import EntityDegree, entity_degrees


@dataclass
class EntityTypeAndDegree(Record):
    id: str
    types: dict[str, int]
    indegree: int
    outdegree: int

    wikipedia_indegree: Optional[int] = None
    wikipedia_outdegree: Optional[int] = None


def entity_types_and_degrees(lang: str = "en") -> Dataset[EntityTypeAndDegree]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.entity_types_and_degrees):
        (
            entity_all_types(lang)
            .get_rdd()
            .map(lambda e: (e.id, e))
            .join(entity_degrees(lang).get_rdd().map(lambda e: (e.id, e)))
            .map(merge_type_degree)
            .map(EntityTypeAndDegree.ser)
            .coalesce(128)
            .saveAsTextFile(
                str(cfg.entity_types_and_degrees),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        cfg.entity_types_and_degrees / "*.gz", deserialize=EntityTypeAndDegree.deser
    )


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
