from dataclasses import dataclass


@dataclass
class WDEntityLabel:
    # contains only label & id of qnode
    __slots__ = ("id", "label")
    id: str
    label: str

    @staticmethod
    def from_dict(o: dict):
        return WDEntityLabel(o["id"], o["label"])

    def to_dict(self):
        return {"id": self.id, "label": self.label}

    @staticmethod
    def from_wdentity_raw(o: dict):
        """Extract wdentity label from wdentity raw dictionary (which is passed to .from_dict to deserialize wdentity object)"""
        return WDEntityLabel(
            id=o["id"], label=o["label"]["lang2value"][o["label"]["lang"]]
        )
