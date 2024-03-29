from __future__ import annotations

import orjson
from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from sm.misc.funcs import filter_duplication


def entity_types() -> Dataset[tuple[str, list[str]]]:
    """Extract types of entities. Mapping from entity id to its type"""
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.entity_types / "*.gz",
        deserialize=orjson.loads,
        name="entity-types",
        dependencies=[entities()],
    )
    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .map(get_instanceof)
            .map(orjson.dumps)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


def get_instanceof(ent: WDEntity) -> tuple[str, list[str]]:
    return (ent.id, filter_duplication(ent.instance_of()))
