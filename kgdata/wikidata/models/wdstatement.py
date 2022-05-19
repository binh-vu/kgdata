from dataclasses import dataclass
from typing import Dict, List, Literal

from kgdata.wikidata.models.wdvalue import WDValueKind, WDValue


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

    @staticmethod
    def from_dict(o):
        o["qualifiers"] = {
            k: [WDValue(**v) for v in vals] for k, vals in o["qualifiers"].items()
        }
        o["value"] = WDValue(**o["value"])
        return WDStatement(**o)

    def to_dict(self):
        return {
            "value": self.value.to_dict(),
            "qualifiers": {
                k: [v.to_dict() for v in vals] for k, vals in self.qualifiers.items()
            },
            "qualifiers_order": self.qualifiers_order,
            "rank": self.rank,
        }
