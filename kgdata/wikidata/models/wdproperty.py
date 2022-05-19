from dataclasses import dataclass
from typing import List, Literal, Set
from kgdata.wikidata.models.wdentity import WDEntity

import orjson

from kgdata.config import WIKIDATA_DIR
from kgdata.wikidata.models.multilingual import (
    MultiLingualString,
    MultiLingualStringList,
)
from sm.misc.deser import deserialize_jl, deserialize_json


@dataclass
class WDProperty:
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
    # wikibase-lexeme, monolingualtext, wikibase-sense, url, wikibase-property,
    # wikibase-form, external-id, time, commonsMedia, quantity, wikibase-item, musical-notation,
    # tabular-data, string, math, geo-shape, globe-coordinate
    datatype: Literal[
        "wikibase-lexeme",
        "monolingualtext",
        "wikibase-sense",
        "url",
        "wikibase-property",
        "wikibase-form",
        "external-id",
        "time",
        "commonsMedia",
        "quantity",
        "wikibase-item",
        "musical-notation",
        "tabular-data",
        "string",
        "math",
        "geo-shape",
        "globe-coordinate",
    ]
    parents: List[str]
    related_properties: List[str]
    equivalent_properties: List[str]
    subjects: List[str]
    inverse_properties: List[str]
    instanceof: List[str]
    ancestors: Set[str]

    @staticmethod
    def from_dict(o):
        o["label"] = MultiLingualString(**o["label"])
        o["description"] = MultiLingualString(**o["description"])
        o["aliases"] = MultiLingualStringList(**o["aliases"])
        o["ancestors"] = set(o["ancestors"])
        return WDProperty(**o)

    @staticmethod
    def from_entity(ent: WDEntity):
        parents = []
        for stmt in ent.props.get("P1647", []):
            assert stmt.value.is_entity_id(stmt.value)
            parents.append(stmt.value.as_entity_id())

        related_properties = []
        for stmt in ent.props.get("P1659", []):
            assert stmt.value.is_entity_id(stmt.value)
            related_properties.append(stmt.value.as_entity_id())

        equivalent_properties = []
        for stmt in ent.props.get("P1628", []):
            if stmt.value.is_entity_id(stmt.value):
                equivalent_properties.append(stmt.value.as_entity_id())
            elif stmt.value.is_string(stmt.value):
                # external url is represented as a string
                equivalent_properties.append(stmt.value.as_string())
            else:
                assert False, f"Unknown type: {stmt.value.to_dict()}"

        subjects = []
        for stmt in ent.props.get("P1629", []):
            assert stmt.value.is_entity_id(stmt.value)
            subjects.append(stmt.value.as_entity_id())

        inverse_properties = []
        for stmt in ent.props.get("P1696", []):
            assert stmt.value.is_entity_id(stmt.value)
            inverse_properties.append(stmt.value.as_entity_id())

        instanceof = []
        for stmt in ent.props.get("P31", []):
            assert stmt.value.is_entity_id(stmt.value)
            instanceof.append(stmt.value.as_entity_id())

        return WDProperty(
            id=ent.id,
            label=ent.label,
            description=ent.description,
            datatype=ent.datatype,  # type: ignore
            aliases=ent.aliases,
            parents=sorted(parents),
            related_properties=sorted(related_properties),
            equivalent_properties=sorted(equivalent_properties),
            subjects=sorted(subjects),
            inverse_properties=sorted(inverse_properties),
            instanceof=sorted(instanceof),
            ancestors=set(),
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
            "ancestors": list(self.ancestors),
        }

    def is_object_property(self):
        return self.datatype == "wikibase-item"

    def is_data_property(self):
        return not self.is_object_property()

    def is_transitive(self):
        return "Q18647515" in self.instanceof
