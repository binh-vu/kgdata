from __future__ import annotations

from typing import Generic, Literal, TypedDict, TypeVar, Union

import orjson
from kgdata.core.models import Value
from rdflib import XSD
from rdflib import Literal as LiteralTerm
from rdflib import URIRef
from sm.namespaces.namespace import KnowledgeGraphNamespace
from typing_extensions import TypeGuard

"""
https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Data_Values is moved to https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html
the new document does not have all types specified in https://www.wikidata.org/wiki/Help:Data_type such as monolingualtext
so keep in mind the type may not be exhausted
"""


WDValueType = Literal[
    "string",
    "wikibase-entityid",
    "time",
    "quantity",
    "monolingualtext",
    "globecoordinate",
]
WD_VALUE_QUANTITY_TYPE_URI = "http://wikiba.se/ontology#Value:Quantity"
WD_VALUE_TIME_TYPE_URI = "http://wikiba.se/ontology#Value:Time"
WD_VALUE_GLOBECOORDINATE_TYPE_URI = "http://wikiba.se/ontology#Value:GlobeCoordinate"


T = TypeVar(
    "T",
    Literal["string"],
    Literal["wikibase-entityid"],
    Literal["time"],
    Literal["quantity"],
    Literal["monolingualtext"],
    Literal["globecoordinate"],
)
V = TypeVar("V", covariant=True)


ValueWikibaseEntityId = TypedDict(
    "ValueWikibaseEntityId",
    {
        "entity-type": Literal["item", "property"],
        "id": str,
        # WARNING: not all entity IDs have a numeric ID – using the full ID is highly recommended.
        "numeric-id": int,
    },
)
ValueGlobeCoordinate = TypedDict(
    "ValueGlobeCoordinate",
    {
        "latitude": float,
        "longitude": float,
        "precision": float,
        # deprecated, No longer used. Will be dropped in the future
        "altitude": None,
        # The URI of a reference globe. This would typically refer to a data item on wikidata.org. This is usually just an indication of the celestial body (e.g. Q2 = earth), but could be more specific, like WGS 84 or ED50.
        "globe": str,
    },
)
ValueQuantity = TypedDict(
    "ValueQuantity",
    {
        # The nominal value of the quantity, as an arbitrary precision decimal string. The string always starts with a character indicating the sign of the value, either “+” or “-”.
        "amount": str,
        # Optionally, the upper bound of the quantity's uncertainty interval, using the same notation as the amount field. If not given or null, the uncertainty (or precision) of the quantity is not known. If the upperBound field is given, the lowerBound field must also be given.
        "upperBound": str,
        # Optionally, the lower bound of the quantity's uncertainty interval, using the same notation as the amount field. If not given or null, the uncertainty (or precision) of the quantity is not known. If the lowerBound field is given, the upperBound field must also be given.
        "lowerBound": str,
        # The URI of a unit (or “1” to indicate a unit-less quantity). This would typically refer to a data item on wikidata.org, e.g. http://www.wikidata.org/entity/Q712226 for “square kilometer”.
        "unit": str,
    },
)
ValueTime = TypedDict(
    "ValueTime",
    {
        # See more: https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html
        "time": str,
        "timezone": int,
        "before": int,
        "after": int,
        "precision": int,
        "calendarmodel": str,
    },
)
ValueMonolingualText = TypedDict(
    "ValueMonolingualText",
    {
        "text": str,
        "language": str,
    },
)


class WDValue(Generic[T, V]):
    __slots__ = ("type", "value")

    def __init__(self, type: T, value: V):
        self.type: T = type
        self.value: V = value

    @staticmethod
    def is_string(value: WDValue) -> TypeGuard[WDValueString]:
        return value.type == "string"

    @staticmethod
    def is_time(value: WDValue) -> TypeGuard[WDValueTime]:
        return value.type == "time"

    @staticmethod
    def is_quantity(value: WDValue) -> TypeGuard[WDValueQuantity]:
        return value.type == "quantity"

    @staticmethod
    def is_mono_lingual_text(value: WDValue) -> TypeGuard[WDValueMonolingualText]:
        return value.type == "monolingualtext"

    @staticmethod
    def is_globe_coordinate(value: WDValue) -> TypeGuard[WDValueGlobeCoordinate]:
        return value.type == "globecoordinate"

    @staticmethod
    def is_entity_id(value: WDValue) -> TypeGuard[WDValueEntityId]:
        return value.type == "wikibase-entityid"

    @staticmethod
    def is_qnode(value: WDValue) -> TypeGuard[WDValueEntityId]:
        return value.type == "wikibase-entityid" and value.value["id"][0] == "Q"

    @staticmethod
    def is_pnode(value: WDValue) -> TypeGuard[WDValueEntityId]:
        return value.type == "wikibase-entityid" and value.value["id"][0] == "P"

    def as_string(self: WDValueString) -> str:
        return self.value

    def as_entity_id(self: WDValueEntityId) -> str:
        return self.value["id"]

    def as_entity_id_safe(self: WDValue) -> str:
        assert WDValue.is_entity_id(self)
        return self.value["id"]

    def as_qnode_id_safe(self: WDValue) -> str:
        assert WDValue.is_qnode(self)
        return self.value["id"]

    def as_pnode_id_safe(self: WDValue) -> str:
        assert WDValue.is_pnode(self)
        return self.value["id"]

    def to_dict(self):
        return {
            "type": self.type,
            "value": self.value,
        }

    def to_tuple(self):
        return (self.type, self.value)

    def to_string_repr(self) -> str:
        return orjson.dumps(self.to_dict()).decode()

    def to_rust_string_repr(self) -> str:
        return orjson.dumps({self.type: self.value}).decode("utf-8")

    def to_rust(self, cls=Value) -> Value:
        if self.is_string(self):
            return cls.string(self.value)
        if self.is_entity_id(self):
            return cls.entity_id(
                self.value["id"], self.value["entity-type"], self.value["numeric-id"]
            )
        if self.is_time(self):
            return cls.time(
                self.value["time"],
                self.value["timezone"],
                self.value["before"],
                self.value["after"],
                self.value["precision"],
                self.value["calendarmodel"],
            )
        if self.is_quantity(self):
            return cls.quantity(
                self.value["amount"],
                self.value.get("lowerBound", None),
                self.value.get("upperBound", None),
                self.value["unit"],
            )
        if self.is_globe_coordinate(self):
            return cls.globe_coordinate(
                self.value["latitude"],
                self.value["longitude"],
                self.value["precision"],
                self.value["altitude"],
                self.value["globe"],
            )
        if self.is_mono_lingual_text(self):
            return cls.monolingual_text(self.value["text"], self.value["language"])
        raise ValueError(f"Unknown type: {self.type}")

    def to_rdf(self, wdns: KnowledgeGraphNamespace) -> URIRef | LiteralTerm:
        if self.is_entity_id(self):
            return URIRef(wdns.id_to_uri(self.as_entity_id()))

        if self.is_string(self):
            return LiteralTerm(self.value, datatype=XSD.string)

        if self.is_mono_lingual_text(self):
            return LiteralTerm(
                self.value["text"], lang=self.value["language"], datatype=XSD.string
            )

        if self.is_time(self):
            return LiteralTerm(
                orjson.dumps(self.value).decode(), datatype=WD_VALUE_TIME_TYPE_URI
            )

        if self.is_quantity(self):
            return LiteralTerm(
                orjson.dumps(self.value).decode(), datatype=WD_VALUE_QUANTITY_TYPE_URI
            )

        if self.is_globe_coordinate(self):
            return LiteralTerm(
                orjson.dumps(self.value).decode(),
                datatype=WD_VALUE_GLOBECOORDINATE_TYPE_URI,
            )

        raise ValueError(f"Unknown type: {self.type}")


WDValueString = WDValue[Literal["string"], str]
WDValueEntityId = WDValue[Literal["wikibase-entityid"], ValueWikibaseEntityId]
WDValueTime = WDValue[Literal["time"], ValueTime]
WDValueQuantity = WDValue[Literal["quantity"], ValueQuantity]
WDValueMonolingualText = WDValue[Literal["monolingualtext"], ValueMonolingualText]
WDValueGlobeCoordinate = WDValue[Literal["globecoordinate"], ValueGlobeCoordinate]

WDValueKind = Union[
    WDValueString,
    WDValueEntityId,
    WDValueTime,
    WDValueQuantity,
    WDValueMonolingualText,
    WDValueGlobeCoordinate,
]


# def type_check(val: WDValueKind):
#     """The function is here to see if the type checker is able to flag error.

#     Uncomment to see the errors.

#     Tested with Pylance and mypy in 2022-05-15.
#     """
#     if WDValue.is_entity_id(val):
#         # val.as_string()
#         val.as_entity_id()

#     if WDValue.is_string(val):
#         val.as_string()
#         # val.as_entity_id()
