from __future__ import annotations

from dataclasses import dataclass

from kgdata.models.ont_class import OntologyClass
from kgdata.wikidata.models.wdentity import WDEntity


@dataclass
class WDClass(OntologyClass):
    @staticmethod
    def from_entity(ent: WDEntity):
        assert ent.datatype is None, (ent.id, ent.datatype)

        parents = []
        for stmt in ent.props.get("P279", []):
            parents.append(stmt.value.as_entity_id_safe())

        properties = []
        for stmt in ent.props.get("P1963", []):
            properties.append(stmt.value.as_entity_id_safe())

        different_froms = []
        for stmt in ent.props.get("P1889", []):
            different_froms.append(stmt.value.as_entity_id_safe())

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
            ancestors={},
        )

    def __str__(self):
        return f"{self.label} ({self.id})"

    def to_base(self):
        return OntologyClass(
            id=self.id,
            label=self.label,
            description=self.description,
            aliases=self.aliases,
            parents=self.parents,
            properties=self.properties,
            different_froms=self.different_froms,
            equivalent_classes=self.equivalent_classes,
            ancestors=self.ancestors,
        )

    def fmt(self):
        return "\n".join(
            [
                f"label: {self.label} ({self.id})",
                f"description: {self.description}",
                f"parents: {self.parents}",
            ]
        )
