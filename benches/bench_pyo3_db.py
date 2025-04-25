"""
Benchmark different ways of calling Rust from Python. Implemented with help from pybench library.

For how to run, checkout `pybench.__main__.py`
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal

import kgbench
import orjson
import serde.jl
from kgdata.wikidata.models import WDEntity
from pybench.base import BenchSetup
from pybench.helper import get_module

# from kgdata_core.bench import EntityDesign1, EntityDesign2

# fmt: off
WD_ENT_FILE = Path(__file__).parent / "resources" / "wdentities.jl"


class Python:
    @staticmethod
    def read_entities(infile: str):
        return serde.jl.deser(infile, cls = WDEntity)


class Rust:
    read_entities = kgbench.read_entities



@dataclass
class BenchPyo3(BenchSetup):
    bench: str
    method: str

    def get_bench_name(self):
        return self.bench

    def get_method_name(self) -> str:
        return self.method

    def get_setup(self):
        module = get_module(__file__)
        return "\n".join(
            [
                f"from {module} import {self.method}, WD_ENT_FILE",
                f"fn = {self.method}.{self.bench}",
                f"wd_ent_file = str(WD_ENT_FILE)"
            ]
        )

    def get_statement(self):
        if self.bench == "read_entities":
            return "fn(wd_ent_file)"

        assert False, f"Unknown bench: {self.bench}"

    @staticmethod
    def iter_configs(default_cfg: dict) -> Iterator[BenchSetup]:
        for bench in [
            "read_entities",
        ]:
            for method in ["Rust", "Python"]:
                yield BenchPyo3(
                    bench=bench,
                    method=method,
                )


if __name__ == "__main__":
    from kgdata.wikidata.db import WikidataDB

    db = WikidataDB(Path(__file__).parent.parent / "data/databases")
    lst = []
    for item in db.entities.values():
        lst.append(item.to_dict())
        if len(lst) > 100:
            break

    serde.jl.ser(lst, WD_ENT_FILE)
