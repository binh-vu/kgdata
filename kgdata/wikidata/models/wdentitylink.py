from __future__ import annotations
from dataclasses import dataclass


@dataclass
class WDEntityWikiLink:
    __slots__ = ("source", "targets")
    source: str  # source entity id
    targets: set[str]  # target entity ids

    @staticmethod
    def from_dict(o: dict):
        return WDEntityWikiLink(o["source"], set(o["targets"]))

    def to_dict(self):
        return {"source": self.source, "targets": list(self.targets)}
