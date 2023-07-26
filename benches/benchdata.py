import random
from pathlib import Path

import orjson
import serde.jl
from kgdata.wikidata.db import WikidataDB

RESOURCE_DIR = Path(__file__).parent / "resources"


def setup_benchdata(n: int, prob: float = 0.1):
    db = WikidataDB.get_instance()

    # sample n entities
    eids = []
    for eid in db.entities:
        eids.append(eid)
        if len(eids) >= int(n / prob):
            break

    sample_eids = random.sample(eids, n)
    data = []
    for eid in sample_eids:
        data.append(db.entities[eid])
    serde.jl.ser(data, RESOURCE_DIR / "wdentities.jl")


if __name__ == "__main__":
    WikidataDB.init("./data/home/databases")
    setup_benchdata(500)
