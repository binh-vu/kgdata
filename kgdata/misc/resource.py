from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Generic, Iterable, TypeVar, Union

import orjson
from kgdata.misc.ntriples_parser import Triple, node_from_dict, node_to_dict
from rdflib import BNode, Literal, URIRef

V = TypeVar("V")


@dataclass
class Record:
    """A base class that provides default serialization and deserialization."""

    def ser(self) -> bytes:
        return orjson.dumps(
            self, option=orjson.OPT_SERIALIZE_DATACLASS, default=orjson_default
        )

    @classmethod
    def deser(cls, o: str):
        return cls(**orjson.loads(o))

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls: type[V], obj: dict) -> V:
        return cls(**obj)


@dataclass
class Resource(Record, Generic[V]):
    __slots__ = ("id", "props")
    id: str
    props: dict[str, list[V]]


@dataclass
class RDFResource(Resource[Union[URIRef, BNode, Literal]]):
    def ser(self) -> bytes:
        return orjson.dumps(
            {
                "id": self.id,
                "props": {
                    k: [node_to_dict(v) for v in vs] for k, vs in self.props.items()
                },
            }
        )

    @classmethod
    def deser(cls, s: str | bytes):
        o = orjson.loads(s)
        return cls(
            id=o["id"],
            props={k: [node_from_dict(v) for v in vs] for k, vs in o["props"].items()},
        )

    def merge(self, resource: RDFResource) -> RDFResource:
        for pid, lst in resource.props.items():
            if pid not in self.props:
                self.props[pid] = lst
            else:
                self.props[pid].extend(
                    (item for item in lst if item not in self.props[pid])
                )
        return self

    @staticmethod
    def from_triples(source: URIRef | BNode, triples: Iterable[Triple]):
        props: dict[str, list] = defaultdict(list)
        for _, pred, obj in triples:
            props[str(pred)].append(obj)
        return RDFResource(str(source), dict(props))


def orjson_default(o):
    if hasattr(o, "to_dict"):
        return o.to_dict()
    raise TypeError(o)
