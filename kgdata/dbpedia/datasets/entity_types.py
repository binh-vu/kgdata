from __future__ import annotations

import orjson
from rdflib import RDF

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.models.entity import Entity


def entity_types(lang: str = "en") -> Dataset[tuple[str, list[str]]]:
    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        cfg.entity_types / "*.gz",
        deserialize=orjson.loads,
        name=f"entity_types/{lang}",
        dependencies=[entities(lang)],
    )

    if not ds.has_complete_data():
        (
            entities(lang)
            .get_extended_rdd()
            .map(get_instanceof)
            .map(orjson.dumps)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


def get_instanceof(ent: Entity) -> tuple[str, list[str]]:
    instanceof = str(RDF.type)
    return (
        ent.id,
        list({str(stmt.value) for stmt in ent.props.get(instanceof, [])}),
    )
