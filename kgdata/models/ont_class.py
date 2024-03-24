from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from rdflib import OWL, RDFS


@dataclass(kw_only=True, slots=True)
class OntologyClass:
    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    parents: list[str]
    # properties of a type, "P1963"
    properties: list[str]
    different_froms: list[str]
    equivalent_classes: list[str]
    ancestors: dict[str, int]

    @staticmethod
    def empty(id: str):
        return OntologyClass(
            id=id,
            label=MultiLingualString.en(id),
            description=MultiLingualString.en(""),
            aliases=MultiLingualStringList({"en": []}, "en"),
            parents=[],
            properties=[],
            different_froms=[],
            equivalent_classes=[],
            ancestors={},
        )

    @classmethod
    def from_dict(cls, obj):
        obj["label"] = MultiLingualString(**obj["label"])
        obj["description"] = MultiLingualString(**obj["description"])
        obj["aliases"] = MultiLingualStringList(**obj["aliases"])
        obj["ancestors"] = obj["ancestors"]
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
            "ancestors": self.ancestors,
        }

    def get_ancestors(
        self, distance: int, classes: Mapping[str, OntologyClass]
    ) -> set[str]:
        """Retrieve all ancestors of this class within a given distance"""
        output = set(self.parents)
        if distance == 1:
            return output
        for parent in self.parents:
            output.update(classes[parent].get_ancestors(distance - 1, classes))
        return output

    def get_distance(self, ancestor: str, classes: Mapping[str, OntologyClass]) -> int:
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

    def is_class_or_subclass_of(self, class_id: str):
        return self.id == class_id or class_id in self.ancestors


def get_default_classes() -> list[OntologyClass]:
    return [
        OntologyClass(
            id=str(OWL.Thing),
            label=MultiLingualString({"en": "Thing"}, "en"),
            description=MultiLingualString(
                {"en": "The class of OWL individuals."}, "en"
            ),
            aliases=MultiLingualStringList({"en": []}, "en"),
            parents=[],
            properties=[],
            different_froms=[],
            equivalent_classes=[],
            ancestors={},
        ),
        OntologyClass(
            id=str(RDFS.Resource),
            label=MultiLingualString({"en": "Resource"}, "en"),
            description=MultiLingualString(
                {"en": "The class resource, everything."}, "en"
            ),
            aliases=MultiLingualStringList({"en": []}, "en"),
            parents=[],
            properties=[],
            different_froms=[],
            equivalent_classes=[],
            ancestors={},
        ),
    ]
