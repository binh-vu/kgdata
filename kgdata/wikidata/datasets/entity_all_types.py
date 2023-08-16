from __future__ import annotations

import math
from functools import partial

from kgdata.dataset import Dataset
from kgdata.dbpedia.datasets.entity_all_types import (
    EntityAllTypes,
    extrapolate_class,
    flip_types,
    merge_type_dist,
    merge_types,
)
from kgdata.spark import are_records_unique, does_result_dir_exist, get_spark_context
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.entity_types import entity_types

# approximated size of data sent to each worker to ensure even distrubuted workload
PARTITION_SIZE = 10000


def entity_all_types(lang="en") -> Dataset[EntityAllTypes]:
    cfg = WikidataDirCfg.get_instance()

    unique_check = False

    if not does_result_dir_exist(cfg.entity_all_types):
        id2count = (
            class_count(lang)
            .get_rdd()
            .filter(lambda tup: tup[1] > PARTITION_SIZE)
            .map(lambda tup: (tup[0], math.ceil(tup[1] / PARTITION_SIZE)))
            .collect()
        )
        id2count = {tup[0]: tup[1] for tup in id2count}

        sc = get_spark_context()
        bc_id2count = sc.broadcast(id2count)

        id2ancestors = (
            classes()
            .get_rdd()
            .flatMap(lambda c: extrapolate_class(c, bc_id2count.value))
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

        unique_check = True

    ds = Dataset(
        file_pattern=cfg.entity_all_types / "*.gz", deserialize=EntityAllTypes.deser
    )
    if unique_check:
        assert are_records_unique(ds.get_rdd(), lambda x: x.id)

    return ds
