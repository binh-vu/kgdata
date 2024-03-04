from __future__ import annotations

from functools import partial

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.models.entity import Entity, EntityLabel


def entity_labels() -> Dataset[EntityLabel]:
    """Extract entities' labels."""
    cfg = DBpediaDirCfg.get_instance()

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


def get_labels(ent: Entity) -> EntityLabel:
    return EntityLabel.from_entity(ent)
