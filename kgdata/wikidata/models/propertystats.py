import glob
import gzip
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Set, Union

import orjson

from kgdata.config import WIKIDATA_DIR
from kgdata.wikidata.deprecated.models.qnode import (
    QNode,
    MultiLingualString,
    MultiLingualStringList,
)
from sm.misc.deser import deserialize_jl, deserialize_json


@dataclass
class WDQuantityPropertyStats:
    id: str
    value: "QuantityStats"
    qualifiers: Dict[str, "QuantityStats"]

    @staticmethod
    def from_dir(
        indir: str = os.path.join(
            WIKIDATA_DIR, "step_2/quantity_prop_stats/quantity_stats"
        )
    ) -> Dict[str, "WDQuantityPropertyStats"]:
        odict = {}
        for infile in glob.glob(os.path.join(indir, "*.gz")):
            with gzip.open(infile, "rb") as f:
                for line in f:
                    data = orjson.loads(line)
                    item = WDQuantityPropertyStats(
                        data["id"],
                        QuantityStats(**data["value"]),
                        {
                            q: QuantityStats(**qstat)
                            for q, qstat in data["qualifiers"].items()
                        },
                    )
                    odict[item.id] = item
        return odict


@dataclass
class QuantityStats:
    units: List[str]
    min: float
    max: float
    mean: float
    std: float
    size: float
    int_size: int
    n_overi36: int
