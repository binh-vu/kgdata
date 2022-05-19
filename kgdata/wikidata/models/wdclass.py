from dataclasses import dataclass
from typing import List, Set
from kgdata.wikidata.models.wdentity import WDEntity


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
    parents: List[str]
    # properties of a type, "P1963"
    properties: List[str]
    different_froms: List[str]
    equivalent_classes: List[str]
    ancestors: Set[str]

    @staticmethod
    def from_dict(o):
        o["label"] = MultiLingualString(**o["label"])
        o["description"] = MultiLingualString(**o["description"])
        o["aliases"] = MultiLingualStringList(**o["aliases"])
        o["ancestors"] = set(o["ancestors"])
        return WDClass(**o)

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
