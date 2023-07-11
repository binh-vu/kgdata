from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import orjson
import serde.byteline
import serde.jl
import serde.textline
from pybench.base import BenchSetup
from pybench.helper import get_module

from kgdata.core import Test

infile = Path(__file__).parent / "resources" / "entity_labels.jl.gz"


class PythonImpl:
    @dataclass
    class Record:
        id: str
        label: dict[str, str]  # mapping from language to label

        def test(self):
            return 5

        def test2(self):
            return int(self.id[1:])

        def test3(self):
            return self.label

    def __init__(self, nrecords: int):
        self.rawrecords = serde.textline.deser(infile)[:nrecords]
        self.records = self.deser()

    def deser(self):
        return [self.Record(**orjson.loads(r)) for r in self.rawrecords]

    def deser_get_id(self):
        size = 0
        for r in self.deser():
            size += len(r.id)
        return size

    def get_id(self):
        size = 0
        for r in self.records:
            size += len(r.id)
        return size

    def contains(self):
        size = 0
        for r in self.records:
            if "en" in r.label:
                size += 1
        return size

    def test(self):
        for r in self.records:
            r.test()

    def test2(self):
        size = 0
        for r in self.records:
            size += r.test2()
        return size

    def test3(self):
        for r in self.records:
            r.test3()


class RustImpl:
    def __init__(self, nrecords: int):
        self.rawrecords = serde.byteline.deser(infile)[:nrecords]
        self.records = self.deser()

    def deser(self):
        return [Test.deser(x) for x in self.rawrecords]

    def deser_get_id(self):
        size = 0
        for r in self.deser():
            size += len(r.id)
        return size

    def get_id(self):
        size = 0
        for r in self.records:
            size += len(r.id)
        return size

    def contains(self):
        size = 0
        for r in self.records:
            if "en" in r.label:
                size += 1
        return size

    def test(self):
        for r in self.records:
            r.test()

    def test2(self):
        size = 0
        for r in self.records:
            size += r.test2()
        return size

    def test3(self):
        for r in self.records:
            r.test3()


@dataclass
class SetupArgs(BenchSetup):
    clsname: str
    method: str
    nrecords: int

    def get_bench_name(self):
        return f"pyo3_wrapper_{self.nrecords}"

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
            "RustImpl",
            "PythonImpl",
        ]:
            for method in ["test"]:
                yield SetupArgs(
                    clsname=clsname,
                    method=method,
                    nrecords=default_cfg.get("nrecords", 100),
                )


if __name__ == "__main__2":
    # make data
    from kgdata.wikidata.db import WikidataDB

    samples = []
    for ent in WikidataDB(
        Path(__file__).parent.parent / "data/databases"
    ).wdentities.values():
        label = ent.label.to_dict()["lang2value"]
        if len(label) > 20:
            samples.append({"id": ent.id, "label": label})
            if len(samples) > 1000:
                break

    serde.jl.ser(samples, infile)
