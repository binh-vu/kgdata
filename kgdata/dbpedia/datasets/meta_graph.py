from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Iterable, Optional

import orjson
from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.dbpedia.datasets.entity_types import entity_types
from kgdata.models.entity import Entity
from rdflib import Literal, URIRef
from sm.misc.funcs import filter_duplication


def meta_graph():
    cfg = DBpediaDirCfg.get_instance()

    ds = Dataset(
        cfg.meta_graph / "*.gz",
        deserialize=partial(deser_from_dict, MetaEntity),
        name="meta-graph",
        dependencies=[entities(), entity_types()],
    )

    if not ds.has_complete_data():

        def join_outlink_and_types(
            g: tuple[str, tuple[Iterable[str], Optional[list[str]]]]
        ):
            (target_ent_id, (source_ent_ids, target_ent_types)) = g
            target_ent_types = target_ent_types or []
            return [
                (source_ent_id, (target_ent_id, target_ent_types))
                for source_ent_id in source_ent_ids
            ]

        def convert_value(value: URIRef | Literal) -> Optional[list[str]]:
            if isinstance(value, URIRef):
                return [str(value)]
            return None

        def is_uri_in_main_ns(uri: URIRef | str) -> bool:
            return any(
                uri.startswith(ns)
                for ns in [
                    "http://dbpedia.org/ontology/",
                    "http://dbpedia.org/property/",
                    "http://dbpedia.org/resource/",
                ]
            )

        def get_raw_meta_entity(entity: Entity) -> tuple[str, MetaEntity]:
            props = {}

            for pid, stmts in entity.props.items():
                if not is_uri_in_main_ns(pid):
                    continue
                meta_stmts = []
                for stmt in stmts:
                    # we only keep values that are in the main URIs
                    if isinstance(stmt.value, URIRef) and not is_uri_in_main_ns(
                        stmt.value
                    ):
                        continue

                    qualifiers = {}
                    for k, vs in stmt.qualifiers.items():
                        vs = [
                            convert_value(v)
                            for v in vs
                            if not isinstance(v, URIRef) or is_uri_in_main_ns(v)
                        ]
                        if len(vs) > 0:
                            qualifiers[k] = vs

                    meta_stmts.append(
                        MetaStatement(
                            value=convert_value(stmt.value),
                            qualifiers=qualifiers,
                        )
                    )

                if len(meta_stmts) > 0:
                    props[pid] = meta_stmts
            return entity.id, MetaEntity(
                classes=filter_duplication(
                    (uri for uri in entity.instance_of() if is_uri_in_main_ns(uri))
                ),
                props=props,
            )

        def join_target_types_meta_entity(
            g: tuple[str, tuple[Iterable[tuple[str, list[str]]], MetaEntity]]
        ) -> MetaEntity:
            (entity_id, (target_ent_and_types, meta_entity)) = g
            target_ent_and_types = list(target_ent_and_types)
            map_target_ent_to_types = dict(target_ent_and_types)
            assert len(map_target_ent_to_types) == len(target_ent_and_types)

            return MetaEntity(
                classes=meta_entity.classes,
                props={
                    pid: [
                        MetaStatement(
                            value=(
                                map_target_ent_to_types[stmt.value[0]]
                                if stmt.value is not None
                                else None
                            ),
                            qualifiers={
                                k: [
                                    map_target_ent_to_types[v[0]]
                                    for v in vs
                                    if v is not None
                                ]
                                for k, vs in stmt.qualifiers.items()
                            },
                        )
                        for stmt in stmts
                    ]
                    for pid, stmts in meta_entity.props.items()
                },
            )

        (
            entities()
            .get_extended_rdd()
            .flatMap(lambda x: [(t, x.id) for t in get_out_neighbors(x)])
            .groupByKey()
            .leftOuterJoin(
                entity_types()
                .get_extended_rdd()
                .map(
                    lambda x: (
                        x[0],
                        filter_duplication(
                            [uri for uri in x[1] if is_uri_in_main_ns(uri)]
                        ),
                    )
                )
            )
            .flatMap(join_outlink_and_types)
            .groupByKey()
            .join(entities().get_extended_rdd().map(get_raw_meta_entity))
            .map(join_target_types_meta_entity)
            .map(lambda x: orjson.dumps(x.to_dict()))
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


@dataclass
class MetaEntity:
    # list of classes that original entity is an instance of
    classes: list[str]
    props: dict[str, list[MetaStatement]]

    def to_dict(self):
        return {
            "classes": self.classes,
            "props": {k: [v.to_dict() for v in vals] for k, vals in self.props.items()},
        }

    @staticmethod
    def from_dict(o):
        return MetaEntity(
            o["classes"],
            {
                k: [MetaStatement.from_dict(v) for v in vals]
                for k, vals in o["props"].items()
            },
        )


# list of classes that target entity is an instance of, None if target entity is a literal
TargetMetaClass = Optional[list[str]]


@dataclass
class MetaStatement:
    value: TargetMetaClass  # either class id or None (for literal)
    qualifiers: dict[str, list[TargetMetaClass]]

    def to_dict(self):
        return {
            "value": self.value,
            "qualifiers": self.qualifiers,
        }

    @staticmethod
    def from_dict(o):
        return MetaStatement(o["value"], o["qualifiers"])


def get_out_neighbors(ent: Entity):
    """Get outgoing neighbors of this entity in the graph"""
    out_neighbors = set()
    for stmts in ent.props.values():
        for stmt in stmts:
            if isinstance(stmt.value, URIRef):
                out_neighbors.add(str(stmt.value))
            for values in stmt.qualifiers.values():
                for value in values:
                    if isinstance(value, URIRef):
                        out_neighbors.add(str(value))
    return out_neighbors
