"""
Benchmark different ways of calling Rust from Python. Implemented with help from pybench library.

For how to run, checkout `pybench.__main__.py`
"""

import serde.jl, orjson
from dataclasses import dataclass
from typing import Iterator, Literal
from pybench.helper import get_module
from pybench.base import BenchSetup
from pathlib import Path
from kgdata.kgdata.bench import EntityDesign1, EntityDesign2

# fmt: off
infile = Path(__file__).parent / "resources" / "wdentities.jl.gz"

@dataclass
class BasePythonObject:
    id: str
    entity_type: Literal["item", "property"]

class Base:
    def __init__(self, nrecords: int):
        self.rawrecords = [orjson.dumps({'id': x['id'], 'entity_type': x['type']}) for x in serde.jl.deser(infile, nlines=nrecords)]
        self.nrecords = nrecords
        self.records: list = []
        self.deser()
    
    def access_string(self):
        count = 0
        for r in self.records:
            # count += sum(ord(c) for c in r.id)
            count += len(r.id)
        return count
    
    def deser(self):
        raise NotImplementedError()


class PythonBaseline(Base):

    def __init__(self, nrecords: int):
        super().__init__(nrecords)
        self.deser()
    
    def deser(self):
        self.records = [BasePythonObject(**orjson.loads(r)) for r in self.rawrecords]

class ReturnString(Base):

    def __init__(self, nrecords: int):
        super().__init__(nrecords)
    
    def deser(self):
        self.records = [EntityDesign1.from_bytes(r) for r in self.rawrecords]

class ReturnPyString(Base):

    def __init__(self, nrecords: int):
        super().__init__(nrecords)
    
    def deser(self):
        self.records = [EntityDesign2.from_bytes(r) for r in self.rawrecords]

# fmt: on


@dataclass
class SetupArgs(BenchSetup):
    clsname: str
    method: str
    nrecords: int

    def get_bench_name(self):
        return f"pyo3_{self.nrecords}"

    def get_run_name(self) -> str:
        return f"{self.clsname}_{self.method}_{self.nrecords}"

    def get_setup(self):
        module = get_module(__file__)
        return "\n".join(
            [
                f"from {module} import {self.clsname}",
                f"obj = {self.clsname}({self.nrecords})",
            ]
        )

    def get_statement(self):
        return f"obj.{self.method}()"

    @staticmethod
    def iter_configs(default_cfg: dict) -> Iterator[BenchSetup]:
        for clsname in [
            "PythonBaseline",
            # "ReturnString",
            "ReturnPyString",
        ]:
            for method in ["access_string"]:
                yield SetupArgs(
                    clsname=clsname,
                    method=method,
                    nrecords=default_cfg.get("nrecords", 100),
                )
