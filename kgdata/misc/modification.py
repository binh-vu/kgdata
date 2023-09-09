from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd


@dataclass
class Modification:
    objid: str
    action: Literal["list:remove", "list:add"]
    attrpath: list[str]
    value: str | int

    def apply(self, obj: dict | object):
        if self.action == "list:remove":
            lst = self.get_item(obj, self.attrpath)
            assert isinstance(lst, list)
            lst.remove(self.value)
        elif self.action == "list:add":
            lst = self.get_item(obj, self.attrpath)
            assert isinstance(lst, list)
            lst.append(self.value)
        else:
            raise NotImplementedError()

    def get_item(self, obj: dict | object, attrpath: list[str]) -> Any:
        if isinstance(obj, dict):
            for attr in attrpath:
                if isinstance(attr, str):
                    assert isinstance(obj, dict)
                    obj = obj[attr]
                else:
                    assert isinstance(attr, int) and isinstance(obj, list)
                    obj = obj[attr]
        else:
            for attr in attrpath:
                if isinstance(attr, str):
                    obj = getattr(obj, attr)
                else:
                    assert isinstance(attr, int) and isinstance(obj, list)
                    obj = obj[attr]
        return obj

    def to_dict(self):
        return {
            "objid": self.objid,
            "action": self.action,
            "attrpath": ".".join((str(x) for x in self.attrpath)),
            "value": self.value,
        }

    @staticmethod
    def from_dict(obj: dict):
        obj["attrpath"] = [
            int(x) if x.isdigit() else x for x in obj["attrpath"].split(".")
        ]
        return Modification(**obj)

    @staticmethod
    def from_tsv(file: Path | str) -> dict[str, list[Modification]]:
        records = pd.read_csv(file, comment="#", delimiter="\t").to_dict("records")
        lst = list(map(Modification.from_dict, records))
        idmap = {}
        for item in lst:
            if item.objid not in idmap:
                idmap[item.objid] = []
            idmap[item.objid].append(item)
        return idmap
