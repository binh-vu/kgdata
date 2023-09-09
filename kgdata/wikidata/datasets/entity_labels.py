from __future__ import annotations

from dataclasses import dataclass
from functools import partial

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.models.entity import EntityLabel
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity


def entity_labels() -> Dataset[EntityLabel]:
    """Extract entities' labels."""
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.entity_labels / "*.gz",
        deserialize=partial(deser_from_dict, EntityLabel),
        name="entity-labels",
        dependencies=[entities()],
    )
    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .map(get_labels)
            .map(ser_to_dict)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


def get_labels(ent: WDEntity) -> EntityLabel:
    return EntityLabel(ent.id, ent.label)
