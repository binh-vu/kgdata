import glob
import gzip
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import orjson

from kgdata.wikidata.config import WikidataDirCfg


@dataclass
class WDQuantityPropertyStats:
    id: str
    value: "QuantityStats"
    qualifiers: Dict[str, "QuantityStats"]

    @staticmethod
    def from_dir(indir: Optional[str] = None) -> Dict[str, "WDQuantityPropertyStats"]:
        if indir is None:
            indir = os.path.join(
                WikidataDirCfg.get_instance().datadir,
                "step_2/quantity_prop_stats/quantity_stats",
            )
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
