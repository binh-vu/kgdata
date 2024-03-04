from functools import lru_cache
from typing import Union

import orjson
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.models.entity import Entity, EntityLabel, EntityMetadata
from rdflib import RDF, RDFS, URIRef

instanceof = str(RDF.type)
subclassof = str(RDFS.subClassOf)
subpropertyof = str(RDFS.subPropertyOf)


@lru_cache()
def entity_metadata(lang: str = "en") -> Dataset[EntityMetadata]:
    """Keep all data of the entities but its properties (set it to empty dictionary)."""

    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        cfg.entity_metadata / lang / "*.gz",
        deserialize=deser_entity_metadata,
        name=f"entity-metadata/{lang}",
        dependencies=[entities(lang)],
    )

    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .map(convert_to_entity_metadata)
            .map(ser_entity_metadata)
            .save_like_dataset(ds)
        )

    return ds


def deser_entity_metadata(b: Union[str, bytes]) -> EntityMetadata:
    return EntityMetadata.from_tuple(orjson.loads(b))


def ser_entity_metadata(ent: EntityMetadata) -> bytes:
    return orjson.dumps(ent.to_tuple())


def convert_to_entity_metadata(ent: Entity) -> EntityMetadata:
    props = {}
    for pid in [instanceof, subclassof, subpropertyof]:
        props[pid] = []
        for stmt in ent.props.get(pid, []):
            if isinstance(stmt.value, URIRef):
                props[pid].append(stmt.value)

    return EntityMetadata(
        id=ent.id,
        label=ent.label,
        description=ent.description,
        aliases=ent.aliases,
        instanceof=props[instanceof],
        subclassof=props[subclassof],
        subpropertyof=props[subpropertyof],
    )
