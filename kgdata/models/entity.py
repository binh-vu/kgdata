from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from kgdata.misc.ntriples_parser import node_from_dict, node_to_dict
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from rdflib import Literal, URIRef


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


def apply_redirection_to_term(
    term: URIRef | Literal, redirection: Mapping[str, str]
) -> URIRef | Literal:
    if isinstance(term, URIRef) and str(term) in redirection:
        return URIRef(redirection[str(term)])
    return term
