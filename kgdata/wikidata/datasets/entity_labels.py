from __future__ import annotations

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity


def entity_labels() -> Dataset[tuple[str, list[str]]]:
    """Extract entities' labels."""
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.entity_labels / "*.gz",
        deserialize=orjson.loads,
        name="entity-labels",
        dependencies=[entities()],
    )
    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .map(get_labels)
            .map(orjson.dumps)
            .save_like_dataset(
                ds, auto_coalesce=True, shuffle=True, max_num_partitions=1024
            )
        )

    return ds


def get_labels(ent: WDEntity) -> dict:
    return {"id": ent.id, "label": ent.label.to_dict()}
