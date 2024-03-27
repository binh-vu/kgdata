from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from operator import add
from typing import Optional

import orjson
from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.meta_graph import MetaEntity, meta_graph


def get_predicate_count_dataset(with_dep: bool = True):
    cfg = WikidataDirCfg.get_instance()

    # have information about the domains and ranges of predicates
    return Dataset(
        cfg.meta_graph_stats / "predicate_count/*.gz",
        deserialize=partial(deser_from_dict, PCount),
        name="meta-graph-stats/predicate-count",
        dependencies=[meta_graph()] if with_dep else [],
    )


def get_predicate_conn_dataset(with_dep: bool = True):
    cfg = WikidataDirCfg.get_instance()

    # have information about the domains and ranges of predicates
    return Dataset(
        cfg.meta_graph_stats / "predicate_conn/*.gz",
        deserialize=partial(deser_from_dict, PConnection),
        name="meta-graph-stats/predicate-conn",
        dependencies=[meta_graph()] if with_dep else [],
    )


def get_predicate_occurrence_dataset(with_dep: bool = True):
    cfg = WikidataDirCfg.get_instance()

    # have information about the domains and ranges of predicates
    return Dataset(
        cfg.meta_graph_stats / "predicate_occurrence/*.gz",
        deserialize=partial(deser_from_dict, POccurrence),
        name="meta-graph-stats/predicate-occurrence",
        dependencies=[meta_graph()] if with_dep else [],
    )


def meta_graph_stats():
    predicate_count_ds = get_predicate_count_dataset()
    # have information about the domains and ranges of predicates
    predicate_conn_ds = get_predicate_conn_dataset()
    predicate_occurrence_ds = get_predicate_occurrence_dataset()

    if not predicate_count_ds.has_complete_data():
        (
            meta_graph()
            .get_extended_rdd()
            .flatMap(lambda x: get_pcount(x))
            .reduceByKey(add)
            .map(lambda x: PCount(x[0], x[1]))
            .map(ser_to_dict)
            .save_like_dataset(predicate_count_ds, auto_coalesce=True)
        )

    if not predicate_conn_ds.has_complete_data():

        def merge_pconnections(p1: PConnection, p2: PConnection):
            p1.freq += p2.freq
            return p1

        (
            meta_graph()
            .get_extended_rdd()
            .flatMap(get_pconnection)
            .map(lambda x: (x.get_key(), x))
            .reduceByKey(merge_pconnections)
            .map(lambda x: orjson.dumps(x[1].to_dict()))
            .save_like_dataset(predicate_conn_ds, auto_coalesce=True)
        )

    if not predicate_occurrence_ds.has_complete_data():
        (
            meta_graph()
            .get_extended_rdd()
            .flatMap(get_poccurrence)
            .reduceByKey(add)
            .map(lambda x: POccurrence(x[0][0], x[0][1], x[1]))
            .map(ser_to_dict)
            .save_like_dataset(predicate_occurrence_ds, auto_coalesce=True)
        )


def get_pcount(meta_entity: MetaEntity):
    out: set[tuple[tuple[str, Optional[str]], int]] = set()
    for prop, stmts in meta_entity.props.items():
        out.add(((prop, None), 1))
        for stmt in stmts:
            for qual in stmt.qualifiers.keys():
                out.add(((prop, qual), 1))

    return list(out)


def get_pconnection(meta_entity: MetaEntity):
    out: dict[tuple, PConnection] = {}
    for prop, stmts in meta_entity.props.items():
        for stmt in stmts:
            for source_type in meta_entity.classes:
                if stmt.value is None:
                    conn = PConnection(
                        prop=prop,
                        qual=None,
                        source_type=source_type,
                        target_type=None,
                        freq=1,
                    )
                    out[conn.get_key()] = conn
                else:
                    for target_type in stmt.value:
                        conn = PConnection(
                            prop=prop,
                            qual=None,
                            source_type=source_type,
                            target_type=target_type,
                            freq=1,
                        )
                        out[conn.get_key()] = conn

            for qual, values in stmt.qualifiers.items():
                for value in values:
                    if value is None:
                        for source_type in meta_entity.classes:
                            conn = PConnection(
                                prop=prop,
                                qual=qual,
                                source_type=source_type,
                                target_type=None,
                                freq=1,
                            )
                            out[conn.get_key()] = conn
                    else:
                        for source_type in meta_entity.classes:
                            for target_type in value:
                                conn = PConnection(
                                    prop=prop,
                                    qual=qual,
                                    source_type=source_type,
                                    target_type=target_type,
                                    freq=1,
                                )
                                out[conn.get_key()] = conn

    return list(out.values())


def get_poccurrence(meta_entity: MetaEntity):
    used_predicates: set[tuple[str, Optional[str]]] = set()
    for prop, stmts in meta_entity.props.items():
        used_predicates.add((prop, None))
        for stmt in stmts:
            for qual in stmt.qualifiers.keys():
                used_predicates.add((prop, qual))

    out: list[
        tuple[tuple[tuple[str, Optional[str]], tuple[str, Optional[str]]], int]
    ] = []
    for p1 in used_predicates:
        for p2 in used_predicates:
            if p1 != p2:
                out.append(((p1, p2), 1))

    return out


@dataclass
class PCount:
    predicate: tuple[str, Optional[str]]
    freq: int

    def to_dict(self):
        return {
            "predicate": self.predicate,
            "freq": self.freq,
        }

    @staticmethod
    def from_dict(d: dict):
        return PCount(
            predicate=d["predicate"],
            freq=d["freq"],
        )


@dataclass
class POccurrence:
    predicate1: tuple[str, Optional[str]]
    predicate2: tuple[str, Optional[str]]
    freq: int

    def to_dict(self):
        return {
            "predicate1": self.predicate1,
            "predicate2": self.predicate2,
            "freq": self.freq,
        }

    @staticmethod
    def from_dict(d: dict):
        return POccurrence(
            predicate1=d["predicate1"],
            predicate2=d["predicate2"],
            freq=d["freq"],
        )


@dataclass
class PConnection:
    prop: str
    qual: Optional[str]
    source_type: str
    target_type: Optional[str]
    freq: int

    def get_key(self):
        return (
            self.prop,
            self.qual,
            self.source_type,
            self.target_type,
        )

    def to_dict(self):
        return {
            "prop": self.prop,
            "qual": self.qual,
            "source_type": self.source_type,
            "target_type": self.target_type,
            "freq": self.freq,
        }

    @staticmethod
    def from_dict(d: dict):
        return PConnection(
            prop=d["prop"],
            qual=d["qual"],
            source_type=d["source_type"],
            target_type=d["target_type"],
            freq=d["freq"],
        )
