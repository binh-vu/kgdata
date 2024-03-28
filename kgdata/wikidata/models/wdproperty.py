from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from kgdata.models.ont_property import OntologyProperty
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdstatement import WDStatement

# wikibase-lexeme, monolingualtext, wikibase-sense, url, wikibase-property,
# wikibase-form, external-id, time, commonsMedia, quantity, wikibase-item, musical-notation,
# tabular-data, string, math, geo-shape, globe-coordinate
WDDataType = Literal[
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


@dataclass(kw_only=True, slots=True)
class WDProperty(OntologyProperty):
    datatype: WDDataType
    constraints: list[WDStatement]

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

        constraints = ent.props.get("P2302", [])
        domains = []
        ranges = []
        for stmt in constraints:
            entid = stmt.value.as_entity_id_safe()
            # subject type constraint
            if entid == "Q21503250":
                try:
                    # domains so it must have class -- if not, it's bad and we can ignore
                    if "P2308" not in stmt.qualifiers:
                        continue
                    # and the relation must be instanceof or (instanceof or subclassof), or subclassof
                    assert "P2309" in stmt.qualifiers, (ent.id, stmt)
                    relations = [
                        x.as_entity_id_safe() for x in stmt.qualifiers["P2309"]
                    ]
                    for relation in relations:
                        assert relation in ["Q21503252", "Q30208840", "Q21514624"], (
                            ent.id,
                            stmt,
                        )
                except:
                    continue
                domains = [x.as_entity_id_safe() for x in stmt.qualifiers["P2308"]]

            # value-type constraint
            if entid == "Q21510865":
                try:
                    # if ranges are classes
                    assert "P2308" in stmt.qualifiers, (ent.id, stmt)
                    assert "P2309" in stmt.qualifiers, (ent.id, stmt)
                    # and the relation must be instanceof or (instanceof or subclassof), or subclassof
                    relations = [
                        x.as_entity_id_safe() for x in stmt.qualifiers["P2309"]
                    ]
                    for relation in relations:
                        assert relation in ["Q21503252", "Q30208840", "Q21514624"], (
                            ent.id,
                            stmt,
                        )
                except:
                    continue
                ranges = [x.as_entity_id_safe() for x in stmt.qualifiers["P2308"]]

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
            domains=domains,
            ranges=ranges,
            inverse_properties=sorted(inverse_properties),
            instanceof=sorted(instanceof),
            ancestors={},
            constraints=constraints,
        )

    def is_object_property(self):
        return self.datatype in {
            "wikibase-item",
            "wikibase-property",
            "wikibase-lexeme",
            "wikibase-sense",
        }

    def is_data_property(self):
        return not self.is_object_property()

    def is_transitive(self):
        return "Q18647515" in self.instanceof

    def to_base(self):
        return OntologyProperty(
            id=self.id,
            label=self.label,
            description=self.description,
            aliases=self.aliases,
            datatype=normalize_wikidata_datatype(self.datatype),
            parents=self.parents,
            related_properties=self.related_properties,
            equivalent_properties=self.equivalent_properties,
            domains=self.domains,
            ranges=self.ranges,
            inverse_properties=self.inverse_properties,
            instanceof=self.instanceof,
            ancestors=self.ancestors,
        )

    def __str__(self):
        return f"{self.label} ({self.id})"

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
            "domains": self.domains,
            "ranges": self.ranges,
            "inverse_properties": self.inverse_properties,
            "instanceof": self.instanceof,
            "ancestors": self.ancestors,
            "constraints": [s.to_dict() for s in self.constraints],
        }

    @classmethod
    def from_dict(cls, obj):
        obj["label"] = MultiLingualString(**obj["label"])
        obj["description"] = MultiLingualString(**obj["description"])
        obj["aliases"] = MultiLingualStringList(**obj["aliases"])
        obj["ancestors"] = obj["ancestors"]
        obj["constraints"] = [WDStatement.from_dict(x) for x in obj["constraints"]]
        return cls(**obj)


def normalize_wikidata_datatype(datatype: WDDataType) -> str:
    if datatype == "wikibase-property" or datatype == "wikibase-item":
        return "http://www.w3.org/2001/XMLSchema#anyURI"
    return datatype


# domains of a property, mapping from the class id to the number of instances of the class having this property
WDPropertyDomains = Mapping[str, int]
# ranges of a property, mapping from the class id to the number of instances of the class having this incoming property
WDPropertyRanges = Mapping[str, int]
