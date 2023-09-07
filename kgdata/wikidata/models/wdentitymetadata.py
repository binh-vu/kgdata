from __future__ import annotations

from dataclasses import dataclass
from typing import List


from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList


# A stripped wikidata entity without its properties (except for instanceof, subclassof, subpropertyof)
@dataclass
class WDEntityMetadata:
    __slots__ = (
        "id",
        "label",
        "description",
        "aliases",
        "instanceof",
        "subclassof",
        "subpropertyof",
    )

    id: str
    label: MultiLingualString
    description: MultiLingualString
    aliases: MultiLingualStringList
    instanceof: List[str]
    subclassof: List[str]
    subpropertyof: List[str]

    def to_tuple(self):
        return (
            self.id,
            self.label.to_tuple(),
            self.description.to_tuple(),
            self.aliases.to_tuple(),
            self.instanceof,
            self.subclassof,
            self.subpropertyof,
        )

    @staticmethod
    def from_tuple(t):
        t[1] = MultiLingualString(t[1][0], t[1][1])
        t[2] = MultiLingualString(t[2][0], t[2][1])
        t[3] = MultiLingualStringList(t[3][0], t[3][1])
        return WDEntityMetadata(t[0], t[1], t[2], t[3], t[4], t[5], t[6])
