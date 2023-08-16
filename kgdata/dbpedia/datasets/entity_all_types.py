from __future__ import annotations

import math
from dataclasses import dataclass
from functools import partial
from random import randrange
from typing import Iterable, Optional

import orjson

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.class_count import class_count
from kgdata.dbpedia.datasets.classes import classes
from kgdata.dbpedia.datasets.entity_types import entity_types
from kgdata.misc.resource import Record
from kgdata.models.ont_class import OntologyClass
from kgdata.spark import are_records_unique, does_result_dir_exist, get_spark_context


@dataclass
class EntityAllTypes(Record):
    id: str
    # mapping from type to distance of the correct types
    types: dict[str, int]


# approximated size of data sent to each worker to ensure even distrubuted workload
PARTITION_SIZE = 10000


def entity_all_types(lang: str = "en") -> Dataset[EntityAllTypes]:
    cfg = DBpediaDirCfg.get_instance()

    unique_check = False

    if not does_result_dir_exist(cfg.entity_all_types):
        sc = get_spark_context()

        id2count = dict(
            class_count(lang)
            .get_rdd_alike()
            .filter(lambda tup: tup[1] > PARTITION_SIZE)
            .map(lambda tup: (tup[0], math.ceil(tup[1] / PARTITION_SIZE)))
            .collect()
        )
        bc_id2count = sc.broadcast(id2count)

        id2ancestors = sc.parallelize(
            classes()
            .get_rdd_alike()
            .flatMap(lambda c: extrapolate_class(c, type_count=id2count))
            .collect()
        )

        (
            entity_types(lang)
            .get_rdd()
            .flatMap(partial(flip_types, type_count=bc_id2count.value))
            .groupByKey()
            .leftOuterJoin(id2ancestors)
            .flatMap(merge_types)
            .groupByKey()
            .map(lambda x: EntityAllTypes(x[0], merge_type_dist(x[1])))
            .map(EntityAllTypes.ser)
            .saveAsTextFile(
                str(cfg.entity_all_types),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    ds = Dataset(
        file_pattern=cfg.entity_all_types / "*.gz", deserialize=EntityAllTypes.deser
    )
    if unique_check:
        assert are_records_unique(ds.get_rdd(), lambda x: x.id)

    return ds


def extrapolate_class(
    cls: OntologyClass, type_count: dict[str, int]
) -> list[tuple[str, dict[str, int]]]:
    ancestors = cls.ancestors
    if cls.id in type_count:
        return [
            (encode_cls_partition(cls.id, i), ancestors)
            for i in range(type_count[cls.id])
        ]

    return [(encode_cls_partition(cls.id), ancestors)]


def flip_types(tup: tuple[str, list[str]], type_count: dict[str, int]):
    eid, types = tup
    out = []

    for t in types:
        if t not in type_count:
            out.append((encode_cls_partition(t), eid))
        else:
            out.append((encode_cls_partition(t, randrange(type_count[t])), eid))
    return out


def merge_types(
    tup: tuple[str, tuple[Iterable[str], Optional[dict[str, int]]]]
) -> list[tuple[str, dict[str, int]]]:
    type_id, (ent_ids, type_ancestors) = tup
    if type_ancestors is None:
        # dbpedia has mixed types, here we only want to keep the types that are in dbpedia ontology
        return []

    new_types = type_ancestors.copy()
    new_types[decode_cls_partition(type_id)[0]] = 0

    return [(ent_id, new_types) for ent_id in ent_ids]


def merge_type_dist(it: Iterable[dict[str, int]]) -> dict[str, int]:
    o = {}
    for dist in it:
        for k, v in dist.items():
            if k not in o:
                o[k] = v
            elif v < o[k]:
                o[k] = v
    return o


def encode_cls_partition(clsid: str, partition: Optional[int] = None) -> str:
    return orjson.dumps((clsid, partition)).decode()


def decode_cls_partition(cls_partition: str) -> tuple[str, int]:
    return orjson.loads(cls_partition)
