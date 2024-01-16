from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import orjson
from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.models.wdentity import WDEntity


def get_main_property_connections_dataset(with_dep: bool = True):
    cfg = WikidataDirCfg.get_instance()

    if with_dep:
        deps = [entities(), entity_types()]
    else:
        deps = []

    return Dataset(
        cfg.main_property_connections / "*.gz",
        deserialize=deser_connection,
        name="property-connections",
        dependencies=deps,
    )


def main_property_connections():
    ds = get_main_property_connections_dataset(with_dep=True)
    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .flatMap(get_prop_connections)
            .combineByKey(
                lambda x: {x.prop: [x]},
                merge_preconn,
                merge_preconns,
            )
            .leftOuterJoin(entity_types().get_extended_rdd())
            .flatMap(lambda x: join_conns_and_types(x[0], x[1][0], x[1][1]))
            .reduceByKey(merge_conns)
            .map(lambda x: orjson.dumps(x[1].to_dict()))
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


instanceof = "P31"
subclass_of = "P279"
subproperty_of = "P1647"

ignored_props = {instanceof, subclass_of, subproperty_of}


def deser_connection(line: Union[str, bytes]) -> PConnection:
    return PConnection.from_dict(orjson.loads(line))


def merge_preconn(collection: dict[str, list[PrePConnection]], conn: PrePConnection):
    if conn.prop not in collection:
        collection[conn.prop] = [conn]
        return collection

    for exconn in collection[conn.prop]:
        if exconn.source_type == conn.source_type:
            assert exconn.target == conn.target
            exconn.freq += conn.freq
            return collection

    collection[conn.prop].append(conn)
    return collection


def merge_preconns(
    a: dict[str, list[PrePConnection]], b: dict[str, list[PrePConnection]]
):
    for prop, conns in b.items():
        if prop not in a:
            a[prop] = conns
            continue
        else:
            # merge two list
            source2conn = {conn.source_type: conn for conn in a[prop]}
            for conn in conns:
                if conn.source_type in source2conn:
                    source2conn[conn.source_type].freq += conn.freq
                else:
                    a[prop].append(conn)

    return a


def join_conns_and_types(
    target: Optional[str],
    collection: dict[str, list[PrePConnection]],
    types: Optional[list[str]],
):
    if target is None:
        return [
            (
                (conn.prop, conn.source_type, ""),
                PConnection(
                    prop=conn.prop,
                    source_type=conn.source_type,
                    target_type=None,
                    freq=conn.freq,
                ),
            )
            for prop, conns in collection.items()
            for conn in conns
        ]

    # so empty types will produce nothing, thus, those recorods are ignored
    if types is None:
        types = []

    out: list[tuple[tuple[str, str, str], PConnection]] = []
    for prop, conns in collection.items():
        for conn in conns:
            for typ in types:
                out.append(
                    (
                        (prop, conn.source_type, typ),
                        PConnection(
                            prop=prop,
                            source_type=conn.source_type,
                            target_type=typ,
                            freq=conn.freq,
                        ),
                    ),
                )
    return out


def merge_conns(a: PConnection, b: PConnection):
    assert a.prop == b.prop
    assert a.source_type == b.source_type
    assert a.target_type == b.target_type
    a.freq += b.freq
    return a


def get_prop_connections(ent: WDEntity):
    domains = {
        stmt.value.as_entity_id_safe(): 1 for stmt in ent.props.get(instanceof, [])
    }
    out: dict[str, list[PrePConnection]] = defaultdict(list)

    for prop, stmts in ent.props.items():
        if prop in ignored_props:
            continue

        conns = set()
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                conns.add(stmt.value.as_entity_id())
            else:
                conns.add(None)

        for domain in domains:
            for conn in conns:
                out[prop].append(
                    PrePConnection(prop=prop, source_type=domain, target=conn, freq=1)
                )

    # reverse the info, so we can get the target type
    return [(conn.target, conn) for pid, conns in out.items() for conn in conns]


@dataclass
class PrePConnection:
    prop: str
    source_type: str
    target: Optional[str]
    freq: int


@dataclass
class PConnection:
    prop: str
    source_type: str
    target_type: Optional[str]
    freq: int

    def to_dict(self):
        return {
            "prop": self.prop,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "freq": self.freq,
        }

    @staticmethod
    def from_dict(d: dict):
        return PConnection(
            prop=d["prop"],
            source_type=d["source_type"],
            target_type=d["target_type"],
            freq=d["freq"],
        )
