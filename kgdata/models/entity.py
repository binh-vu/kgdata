from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from kgdata.misc.ntriples_parser import node_from_dict, node_to_dict
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from rdflib import RDF, Literal, URIRef

RDF_TYPE = str(RDF.type)


@dataclass
class Entity:
    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    props: dict[str, list[Statement]]

    def to_dict(self):
        return {
            "id": self.id,
            "label": self.label.to_dict(),
            "description": self.description.to_dict(),
            "aliases": self.aliases.to_dict(),
            "props": {k: [v.to_dict() for v in vals] for k, vals in self.props.items()},
        }

    @classmethod
    def from_dict(cls, o: dict):
        props = {
            k: [Statement.from_dict(v) for v in vals] for k, vals in o["props"].items()
        }
        label = MultiLingualString(**o["label"])
        description = MultiLingualString(**o["description"])
        aliases = MultiLingualStringList(**o["aliases"])
        return cls(
            id=o["id"],
            label=label,
            description=description,
            aliases=aliases,
            props=props,
        )

    def apply_redirection(self, redirection: Mapping[str, str]) -> Entity:
        return Entity(
            id=redirection.get(self.id, self.id),
            label=self.label,
            description=self.description,
            aliases=self.aliases,
            props={
                pid: [stmt.apply_redirection(redirection) for stmt in stmts]
                for pid, stmts in self.props.items()
            },
        )

    def get_object_prop_value(self, prop: str) -> list[str]:
        lst = []
        for stmt in self.props.get(prop, []):
            if isinstance(stmt.value, URIRef):
                lst.append(str(stmt.value))
        return lst

    def instance_of(self, instanceof: str = RDF_TYPE):
        return self.get_object_prop_value(instanceof)


@dataclass
class Statement:
    value: URIRef | Literal
    qualifiers: dict[str, list[URIRef | Literal]]
    qualifiers_order: list[str]

    def to_dict(self):
        return {
            "value": node_to_dict(self.value),
            "qualifiers": {
                k: [node_to_dict(v) for v in lst] for k, lst in self.qualifiers.items()
            },
            "qualifiers_order": self.qualifiers_order,
        }

    @classmethod
    def from_dict(cls, o: dict):
        o["value"] = node_from_dict(o["value"])
        o["qualifiers"] = {
            k: [node_from_dict(v) for v in lst] for k, lst in o["qualifiers"].items()
        }
        return cls(**o)

    def apply_redirection(self, redirection: Mapping[str, str]) -> Statement:
        return Statement(
            value=apply_redirection_to_term(self.value, redirection),
            qualifiers={
                k: [apply_redirection_to_term(v, redirection) for v in lst]
                for k, lst in self.qualifiers.items()
            },
            qualifiers_order=self.qualifiers_order,
        )


@dataclass
class EntityLabel:
    __slots__ = ("id", "label")
    id: str
    label: str

    @staticmethod
    def from_dict(o: dict):
        return EntityLabel(o["id"], o["label"])

    def to_dict(self):
        return {"id": self.id, "label": self.label}

    @staticmethod
    def from_entity(ent: Entity):
        return EntityLabel(ent.id, str(ent.label))


@dataclass
class EntityMultiLingualLabel:
    id: str
    label: MultiLingualString

    @staticmethod
    def from_dict(obj: dict):
        return EntityLabel(obj["id"], MultiLingualString.from_dict(obj["label"]))

    def to_dict(self):
        return {"id": self.id, "label": self.label.to_dict()}


@dataclass
class EntityOutLinks:
    id: str  # source entity id
    targets: set[str]  # target entity id

    @staticmethod
    def from_dict(obj: dict):
        return EntityOutLinks(obj["id"], set(obj["targets"]))

    def to_dict(self):
        # sort targets for consistency -- otherwise, checksums will be different
        return {"id": self.id, "targets": sorted(self.targets)}


@dataclass
class EntityMetadata:

    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    instanceof: list[str]
    subclassof: list[str]
    subpropertyof: list[str]

    def to_tuple(self):
        return (
            self.id,
            self.label.to_tuple(),
            self.description.to_tuple(),
            self.aliases.to_tuple(),
            self.instanceof,
            self.subclassof,
            self.subpropertyof,
        )

    @staticmethod
    def from_tuple(t):
        t[1] = MultiLingualString(t[1][0], t[1][1])
        t[2] = MultiLingualString(t[2][0], t[2][1])
        t[3] = MultiLingualStringList(t[3][0], t[3][1])
        return EntityMetadata(t[0], t[1], t[2], t[3], t[4], t[5], t[6])


def apply_redirection_to_term(
    term: URIRef | Literal, redirection: Mapping[str, str]
) -> URIRef | Literal:
    if isinstance(term, URIRef) and str(term) in redirection:
        return URIRef(redirection[str(term)])
    return term
