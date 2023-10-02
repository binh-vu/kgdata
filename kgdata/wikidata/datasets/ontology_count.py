from __future__ import annotations

from functools import lru_cache

import orjson

from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.property_count import property_count


def ontology_count() -> Dataset[tuple[str, int]]:
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        cfg.ont_count / "*.gz",
        deserialize=orjson.loads,
        name="ont-count",
        dependencies=[class_count(), property_count()],
    )

    if not ds.has_complete_data():
        # duplicate the data here as we do not have much data in the count
        (
            class_count()
            .get_extended_rdd()
            .union(property_count().get_extended_rdd())
            .map(orjson.dumps)
            .save_like_dataset(ds)
        )
        assert ds.get_extended_rdd().is_unique(lambda x: x[0])
    return ds
