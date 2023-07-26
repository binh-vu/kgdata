from operator import add
from typing import Tuple

import orjson
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, saveAsSingleTextFile
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types, get_instanceof
from kgdata.wikidata.datasets.properties import properties
from kgdata.wikidata.models import WDEntity


def property_count(lang="en") -> Dataset[Tuple[str, int]]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.property_count):
        rdd = entities(lang).get_rdd().map(get_property)

        property_count = rdd.flatMap(lambda x: [(c, 1) for c in x[1]]).reduceByKey(add)
        (
            properties(lang)
            .get_rdd()
            .map(lambda x: (x.id, 0))
            .leftOuterJoin(property_count)
            .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else 0))
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.property_count),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    ds = Dataset(cfg.property_count / "*.gz", deserialize=orjson.loads)

    if not (cfg.property_count / "../property_count_sorted.tsv").exists():
        rdd = (
            properties(lang)
            .get_rdd()
            .map(lambda x: (x.id, str(x.label)))
            .join(ds.get_rdd())
            .map(lambda x: (x[0], x[1][0], x[1][1]))
            .sortBy(lambda x: x[2], ascending=False)
            .map(lambda x: "\t".join([str(y) for y in x]))
        )

        saveAsSingleTextFile(
            rdd, str(cfg.property_count / "../property_count_sorted.tsv"), shuffle=False
        )

    return ds


def get_property(ent: WDEntity):
    return ent.id, sorted(ent.props.keys())
