from __future__ import annotations

from functools import lru_cache
from operator import add

import orjson

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.classes import classes
from kgdata.dbpedia.datasets.entity_types import entity_types


@lru_cache()
def class_count(lang="en") -> Dataset[tuple[str, int]]:
    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        cfg.class_count / "*.gz",
        deserialize=orjson.loads,
        name=f"class-count/{lang}",
        dependencies=[entity_types(lang), classes()],
    )

    if not ds.has_complete_data():
        class_count = (
            entity_types(lang)
            .get_extended_rdd()
            .flatMap(lambda x: [(c, 1) for c in x[1]])
            .reduceByKey(add)
        )
        (
            classes()
            .get_extended_rdd()
            .map(lambda x: (x.id, 0))
            .leftOuterJoin(class_count)
            .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else 0))
            .map(orjson.dumps)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    count_file = cfg.class_count / f"../{cfg.class_count.name}_sorted.tsv"
    if not count_file.exists():
        (
            classes()
            .get_extended_rdd()
            .map(lambda x: (x.id, str(x.label)))
            .join(ds.get_extended_rdd())
            .map(lambda x: (x[0], x[1][0], x[1][1]))
            .sortBy(lambda x: x[2], ascending=False)
            .map(lambda x: "\t".join([str(y) for y in x]))
            .save_as_single_text_file(count_file)
        )

    return ds
