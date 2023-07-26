from operator import add
from typing import Tuple

import orjson
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, saveAsSingleTextFile
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types, get_instanceof


def class_count(lang="en") -> Dataset[Tuple[str, int]]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.class_count):
        ds = entity_types(lang)
        if ds.does_exist():
            rdd = ds.get_rdd()
        else:
            rdd = entities(lang).get_rdd().map(get_instanceof)

        class_count = rdd.flatMap(lambda x: [(c, 1) for c in x[1]]).reduceByKey(add)
        (
            classes(lang)
            .get_rdd()
            .map(lambda x: (x.id, 0))
            .leftOuterJoin(class_count)
            .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else 0))
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.class_count),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    ds = Dataset(cfg.class_count / "*.gz", deserialize=orjson.loads)

    if not (cfg.class_count / "../class_count_sorted.tsv").exists():
        rdd = (
            classes(lang)
            .get_rdd()
            .map(lambda x: (x.id, str(x.label)))
            .join(ds.get_rdd())
            .map(lambda x: (x[0], x[1][0], x[1][1]))
            .sortBy(lambda x: x[2], ascending=False)
            .map(lambda x: "\t".join([str(y) for y in x]))
        )

        saveAsSingleTextFile(
            rdd, str(cfg.class_count / "../class_count_sorted.tsv"), shuffle=False
        )

    return ds
