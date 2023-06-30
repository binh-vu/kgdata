import copy
from pathlib import Path
import orjson
from typing import *
from sm.prelude import M
from kgdata.wikidata.models import WDEntity
from kgdata.wikidata.models.multilingual import MultiLingualString
from hugedict.hugedict.rocksdb import deserent, serent
from dataclasses import dataclass
import serde.jl
from timer import Timer


@dataclass
class TestEnt:
    __slots__ = (
        "id",
        "type",
        "datatype",
        "label",
        "description",
    )

    id: str
    type: Literal["item", "property"]
    label: MultiLingualString
    datatype: Optional[str]
    description: MultiLingualString

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "label": self.label.to_dict(),
            "datatype": self.datatype,
            "description": self.description.to_dict(),
        }

    @staticmethod
    def from_dict(o):
        o["label"] = MultiLingualString(**o["label"])
        o["description"] = MultiLingualString(**o["description"])
        return TestEnt(**o)


data_dir = Path(__file__).parent.parent / "data"
data_dir / "entities"
file = list((data_dir / "entities").glob("*.gz"))[0]

obj = serde.jl.deser(file, n_lines=1)[0]
ent = WDEntity.from_dict(copy.deepcopy(obj))
test_ent = TestEnt.from_dict(
    {k: obj[k] for k in ["id", "type", "datatype", "label", "description"]}
)

with Timer().watch_and_report("rust dumps", precision=7):
    bin = serent(ent)

with Timer().watch_and_report("rust loads", precision=7):
    ent2 = deserent(bin)

with Timer().watch_and_report("orjson dumps", precision=7):
    bjson = orjson.dumps(test_ent.to_dict())

with Timer().watch_and_report("orjson loads", precision=7):
    test_ent2 = orjson.loads(bjson)

print(">>>", M.percentage(len(bin), len(bjson)))
