from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList


@dataclass
class OntologyProperty:
    __slots__ = (
        "id",
        "label",
        "description",
        "aliases",
        "datatype",
        "parents",
        "related_properties",
        "equivalent_properties",
        "subjects",
        "inverse_properties",
        "instanceof",
        "ancestors",
    )
    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    datatype: str
    parents: list[str]
    related_properties: list[str]
    equivalent_properties: list[str]
    subjects: list[str]
    inverse_properties: list[str]
    instanceof: list[str]
    ancestors: dict[str, int]

    @staticmethod
    def empty(id: str):
        return OntologyProperty(
            id=id,
            label=MultiLingualString.en(id),
            description=MultiLingualString.en(""),
            aliases=MultiLingualStringList({"en": []}, "en"),
            datatype="",
            parents=[],
            related_properties=[],
            equivalent_properties=[],
            subjects=[],
            inverse_properties=[],
            instanceof=[],
            ancestors={},
        )

    @classmethod
    def from_dict(cls, obj):
        obj["label"] = MultiLingualString(**obj["label"])
        obj["description"] = MultiLingualString(**obj["description"])
        obj["aliases"] = MultiLingualStringList(**obj["aliases"])
        obj["ancestors"] = obj["ancestors"]
        return cls(**obj)

    def get_ancestors(
        self, distance: int, props: Mapping[str, OntologyProperty]
    ) -> set[str]:
        output = set(self.parents)
        if distance == 1:
            return output
        for parent in self.parents:
            output.update(props[parent].get_ancestors(distance - 1, props))
        return output

    def get_distance(self, ancestor: str, props: Mapping[str, OntologyProperty]) -> int:
        """Get distance from this property to its ancestor property. Return -1 if ancestor is not an ancestor of this property."""
        if ancestor not in self.ancestors:
            return -1

        if ancestor in self.parents:
            return 1

        return 1 + min(
            d
            for parent in self.parents
            if (d := props[parent].get_distance(ancestor, props)) != -1
        )

    def to_dict(self):
        return {
            "id": self.id,
            "label": self.label.to_dict(),
            "description": self.description.to_dict(),
            "datatype": self.datatype,
            "aliases": self.aliases.to_dict(),
            "parents": self.parents,
            "related_properties": self.related_properties,
            "equivalent_properties": self.equivalent_properties,
            "subjects": self.subjects,
            "inverse_properties": self.inverse_properties,
            "instanceof": self.instanceof,
            "ancestors": self.ancestors,
        }

    def is_object_property(self):
        return self.datatype == "http://www.w3.org/2001/XMLSchema#anyURI"

    def is_data_property(self):
        return not self.is_object_property()
