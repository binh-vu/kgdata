from __future__ import annotations

from dataclasses import dataclass
from functools import partial

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.models.entity import Entity
from kgdata.models.multilingual import MultiLingualString


@dataclass
class EntityLabel:
    id: str
    label: MultiLingualString

    @staticmethod
    def from_dict(obj: dict):
        return EntityLabel(obj["id"], MultiLingualString.from_dict(obj["label"]))

    def to_dict(self):
        return {"id": self.id, "label": self.label.to_dict()}


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
    return EntityLabel(ent.id, ent.label)
