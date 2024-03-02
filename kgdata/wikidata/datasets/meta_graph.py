from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Iterable, Optional

import orjson
from sm.misc.funcs import filter_duplication

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_outlinks import entity_outlinks
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdvalue import WDValue, WDValueKind


def meta_graph():
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.meta_graph / "*.gz",
        deserialize=partial(deser_from_dict, MetaEntity),
        name="meta-graph",
        dependencies=[entities(), entity_outlinks(), entity_types()],
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

        def convert_wdvalue(value: WDValueKind) -> Optional[list[str]]:
            if WDValue.is_entity_id(value):
                return [value.as_entity_id()]
            else:
                return None

        def get_raw_meta_entity(entity: WDEntity) -> tuple[str, MetaEntity]:
            props = {}

            for pid, stmts in entity.props.items():
                meta_stmts = []
                for stmt in stmts:
                    meta_stmts.append(
                        MetaStatement(
                            value=convert_wdvalue(stmt.value),
                            qualifiers={
                                k: [convert_wdvalue(v) for v in vs]
                                for k, vs in stmt.qualifiers.items()
                            },
                        )
                    )
                props[pid] = meta_stmts
            return entity.id, MetaEntity(
                classes=filter_duplication(entity.instance_of()), props=props
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
            entity_outlinks()
            .get_extended_rdd()
            .flatMap(lambda x: [(t, x.id) for t in x.targets])
            .groupByKey()
            .leftOuterJoin(entity_types().get_extended_rdd())
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
