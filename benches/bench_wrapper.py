from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import orjson
import serde.jl
from pybench.base import BenchSetup
from pybench.helper import get_module

infile = Path(__file__).parent / "resources" / "entity_labels.jl.gz"


class PythonImpl:
    @dataclass
    class Record:
        id: str
        label: dict[str, str]  # mapping from language to label

    def __init__(self, nrecords: int):
        self.rawrecords = [
            orjson.dumps({"id": x["id"], "label": x["label"]})
            for x in serde.jl.deser(infile, nlines=nrecords)
        ]

    def deser(self):
        self.records = [self.Record(**orjson.loads(r)) for r in self.rawrecords]


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
            "PythonImpl",
            # "RustImpl",
        ]:
            for method in ["deser"]:
                yield SetupArgs(
                    clsname=clsname,
                    method=method,
                    nrecords=default_cfg.get("nrecords", 100),
                )


if __name__ == "__main__":
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
