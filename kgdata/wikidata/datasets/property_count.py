from operator import add
from typing import Tuple

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.properties import properties
from kgdata.wikidata.models import WDEntity


def property_count() -> Dataset[Tuple[str, int]]:
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        cfg.property_count / "*.gz",
        deserialize=orjson.loads,
        name="property-count",
        dependencies=[entities(), properties()],
    )

    if not does_result_dir_exist(cfg.property_count):
        rdd = entities().get_extended_rdd().map(get_property)

        property_count = rdd.flatMap(lambda x: [(c, 1) for c in x[1]]).reduceByKey(add)
        (
            properties()
            .get_extended_rdd()
            .map(lambda x: (x.id, 0))
            .leftOuterJoin(property_count)
            .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else 0))
            .map(orjson.dumps)
            .save_like_dataset(ds)
        )

    if not (cfg.property_count / "../property_count_sorted.tsv").exists():
        (
            properties()
            .get_extended_rdd()
            .map(lambda x: (x.id, str(x.label)))
            .join(ds.get_extended_rdd())
            .map(lambda x: (x[0], x[1][0], x[1][1]))
            .sortBy(lambda x: x[2], ascending=False)
            .map(lambda x: "\t".join([str(y) for y in x]))
            .save_as_single_text_file(
                cfg.property_count / "../property_count_sorted.tsv"
            )
        )

    return ds


def get_property(ent: WDEntity):
    return ent.id, sorted(ent.props.keys())
    return ent.id, sorted(ent.props.keys())
