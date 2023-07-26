from __future__ import annotations

from dataclasses import dataclass

from kgdata.misc.ntriples_parser import node_from_dict, node_to_dict
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from rdflib import BNode, Literal, URIRef


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
