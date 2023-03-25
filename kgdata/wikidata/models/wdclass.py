from __future__ import annotations
from dataclasses import dataclass
from kgdata.wikidata.models.wdentity import WDEntity
from typing import Mapping

from kgdata.wikidata.models.multilingual import (
    MultiLingualString,
    MultiLingualStringList,
)


@dataclass
class WDClass:
    __slots__ = (
        "id",
        "label",
        "description",
        "aliases",
        "parents",
        "properties",
        "different_froms",
        "equivalent_classes",
        "ancestors",
    )
    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    parents: list[str]
    # properties of a type, "P1963"
    properties: list[str]
    different_froms: list[str]
    equivalent_classes: list[str]
    ancestors: set[str]

    @classmethod
    def from_dict(cls, obj):
        obj["label"] = MultiLingualString(**obj["label"])
        obj["description"] = MultiLingualString(**obj["description"])
        obj["aliases"] = MultiLingualStringList(**obj["aliases"])
        obj["ancestors"] = set(obj["ancestors"])
        return cls(**obj)

    def to_dict(self):
        return {
            "id": self.id,
            "label": self.label.to_dict(),
            "description": self.description.to_dict(),
            "aliases": self.aliases.to_dict(),
            "parents": self.parents,
            "properties": self.properties,
            "different_froms": self.different_froms,
            "equivalent_classes": self.equivalent_classes,
            "ancestors": list(self.ancestors),
        }

    def get_ancestors(self, distance: int, classes: Mapping[str, WDClass]) -> set[str]:
        """Retrieve all ancestors of this class within a given distance"""
        output = set(self.parents)
        if distance == 1:
            return output
        for parent in self.parents:
            output.update(classes[parent].get_ancestors(distance - 1, classes))
        return output

    def get_distance(self, ancestor: str, classes: Mapping[str, WDClass]) -> int:
        """Get distance from this class to its ancestor class. Return -1 if ancestor is not an ancestor of this class."""
        if ancestor not in self.ancestors:
            return -1

        if ancestor in self.parents:
            return 1

        return 1 + min(
            d
            for parent in self.parents
            if (d := classes[parent].get_distance(ancestor, classes)) != -1
        )

    @staticmethod
    def from_entity(ent: WDEntity):
        assert ent.datatype is None

        parents = []
        for stmt in ent.props.get("P279", []):
            assert stmt.value.is_entity_id(stmt.value)
            parents.append(stmt.value.as_entity_id())

        properties = []
        for stmt in ent.props.get("P1963", []):
            assert stmt.value.is_entity_id(stmt.value)
            properties.append(stmt.value.as_entity_id())

        different_froms = []
        for stmt in ent.props.get("P1889", []):
            assert stmt.value.is_entity_id(stmt.value)
            different_froms.append(stmt.value.as_entity_id())

        equivalent_classes = []
        for stmt in ent.props.get("P1709", []):
            assert stmt.value.is_string(stmt.value)
            equivalent_classes.append(stmt.value.as_string())

        return WDClass(
            id=ent.id,
            label=ent.label,
            description=ent.description,
            aliases=ent.aliases,
            parents=sorted(parents),
            properties=sorted(properties),
            different_froms=sorted(different_froms),
            equivalent_classes=sorted(equivalent_classes),
            ancestors=set(),
        )

    def __str__(self):
        return f"{self.label} ({self.id})"
