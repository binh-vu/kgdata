from dataclasses import dataclass
from typing import Dict, List, Literal

from kgdata.models.entity import Statement
from kgdata.wikidata.models.wdvalue import WDValue, WDValueKind
from rdflib import URIRef
from sm.namespaces.namespace import KnowledgeGraphNamespace


@dataclass
class WDStatement:
    __slots__ = ("value", "qualifiers", "qualifiers_order", "rank")

    value: WDValueKind
    # mapping from qualifier id into data value
    qualifiers: Dict[str, List[WDValueKind]]
    # list of qualifiers id that records the order (as dict lacks of order)
    qualifiers_order: List[str]
    # rank of a statement
    rank: Literal["normal", "deprecated", "preferred"]

    def to_dict(self):
        return {
            "value": self.value.to_dict(),
            "qualifiers": {
                k: [v.to_dict() for v in vals] for k, vals in self.qualifiers.items()
            },
            "qualifiers_order": self.qualifiers_order,
            "rank": self.rank,
        }

    @staticmethod
    def from_dict(o):
        o["qualifiers"] = {
            k: [WDValue(**v) for v in vals] for k, vals in o["qualifiers"].items()
        }
        o["value"] = WDValue(**o["value"])
        return WDStatement(**o)

    def to_tuple(self):
        return [
            self.value.to_tuple(),
            {k: [v.to_tuple() for v in vals] for k, vals in self.qualifiers.items()},
            self.qualifiers_order,
            self.rank,
        ]

    @staticmethod
    def from_tuple(o):
        o[0] = WDValue(o[0][0], o[0][1])
        for vals in o[1].values():
            for i, v in enumerate(vals):
                vals[i] = WDValue(v[0], v[1])
        return WDStatement(o[0], o[1], o[2], o[3])

    def to_rdf(self, kgns: KnowledgeGraphNamespace) -> Statement:
        return Statement(
            value=self.value.to_rdf(kgns),
            qualifiers={
                kgns.id_to_uri(qid): [qval.to_rdf(kgns) for qval in qvals]
                for qid, qvals in self.qualifiers.items()
            },
            qualifiers_order=[
                URIRef(kgns.id_to_uri(qid)) for qid in self.qualifiers_order
            ],
        )
