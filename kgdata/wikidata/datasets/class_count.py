from typing import Tuple
from operator import add
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types, get_instanceof
import orjson


def class_count(lang="en") -> Dataset[Tuple[str, int]]:
    cfg = WDDataDirCfg.get_instance()

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

    # return Dataset(cfg.class_count / "*.gz", deserialize=orjson.loads)
    ds = Dataset(cfg.class_count / "*.gz", deserialize=orjson.loads)
    rdd = (
        classes(lang)
        .get_rdd()
        .map(lambda x: (x.id, x.label))
        .join(ds.get_rdd())
        .map(lambda x: [x[0], x[1][0], x[1][1]])
        .sortBy(lambda x: x[2], ascending=True)
        .map(lambda x: "\t".join([str(y) for y in x]))
    )

    saveAsSingleTextFile(
        rdd, str(cfg.class_count / "../class_count_sorted.tsv"), shuffle=False
    )
    # print(ds.get_rdd().sortBy(lambda x: x[1], ascending=False).take(100))
    # print(ds.get_rdd().sortBy(lambda x: x[1], ascending=True).take(1000)[900:])

    return ds
