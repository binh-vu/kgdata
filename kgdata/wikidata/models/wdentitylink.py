from dataclasses import dataclass


@dataclass
class WDEntityWikiLink:
    __slots__ = ("source", "targets")
    source: str  # source entity id
    targets: list[str]  # target entity ids

    @staticmethod
    def from_dict(o: dict):
        return WDEntityWikiLink(o["source"], o["targets"])

    def to_dict(self):
        return {"source": self.source, "targets": self.targets}
