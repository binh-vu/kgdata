from __future__ import annotations

import math
from random import randrange
from typing import Iterable, Optional

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import are_records_unique, does_result_dir_exist, get_spark_context
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.models.wdclass import WDClass


def entity_all_types(lang="en") -> Dataset[tuple[str, list[str]]]:
    cfg = WikidataDirCfg.get_instance()

    unique_check = False

    if not does_result_dir_exist(cfg.entity_all_types):
        # if the number of records of a class exceeds this threshold, we will
        # partition records of the class to make workloads even for each workers
        threshold = 10000

        type_and_threshold = (
            class_count(lang)
            .get_rdd()
            .filter(lambda tup: tup[1] > threshold)
            .map(lambda tup: (tup[0], math.ceil(tup[1] / threshold)))
            .collect()
        )
        type_and_threshold = {tup[0]: tup[1] for tup in type_and_threshold}

        sc = get_spark_context()
        bc_type_and_threshold = sc.broadcast(type_and_threshold)

        cls_ancestors = (
            classes()
            .get_rdd()
            .flatMap(lambda c: extrapolate_class(c, bc_type_and_threshold.value))
        )

        (
            entity_types(lang)
            .get_rdd()
            .flatMap(lambda tup: flip_types(tup, bc_type_and_threshold.value))
            .groupByKey()
            .leftOuterJoin(cls_ancestors)
            .flatMap(merge_types)
            .groupByKey()
            .map(lambda x: (x[0], list(set(x[1]))))
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.entity_all_types),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

        unique_check = True

    ds = Dataset(file_pattern=cfg.entity_all_types / "*.gz", deserialize=orjson.loads)
    if unique_check:
        assert are_records_unique(ds.get_rdd(), lambda x: x[0])

    return ds


def extrapolate_class(
    cls: WDClass, type_count: dict[str, int]
) -> list[tuple[str, list[str]]]:
    ancestors = list(cls.ancestors)
    if cls.id in type_count:
        return [(cls.id + "_" + str(i), ancestors) for i in range(type_count[cls.id])]

    return [(cls.id, ancestors)]


def flip_types(tup: tuple[str, list[str]], type_count: dict[str, int]):
    eid, types = tup
    out = []

    for t in types:
        if t not in type_count:
            out.append((t, eid))
        else:
            out.append((t + "_" + str(randrange(type_count[t])), eid))
    return out


def merge_types(tup: tuple[str, tuple[Iterable[str], Optional[list[str]]]]):
    type_id, (ent_ids, type_ancestors) = tup
    assert type_ancestors is not None

    new_types = type_ancestors.copy()
    new_types.append(type_id.split("_", 1)[0])

    return [(ent_id, t) for ent_id in ent_ids for t in new_types]
