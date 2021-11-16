import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from pickle import load
from typing import List, Dict, Set, Union, Generator
from loguru import logger

import orjson

from kgdata.config import WIKIDATA_DIR
from sm.misc import deserialize_json, deserialize_jl
from kgdata.wikidata.models.qnode import (
    QNode,
    MultiLingualString,
    MultiLingualStringList,
)


@dataclass
class WDClass:
    __slots__ = (
        "id",
        "label",
        "description",
        "datatype",
        "aliases",
        "parents",
        "properties",
        "different_froms",
        "equivalent_classes",
        "parents_closure",
    )
    id: str
    label: MultiLingualString
    description: MultiLingualString
    datatype: str
    aliases: MultiLingualStringList
    parents: List[str]
    properties: List[str]
    different_froms: List[str]
    equivalent_classes: List[str]
    # not include itself
    parents_closure: Set[str]

    @staticmethod
    def from_file(
        indir: Union[str, Path] = os.path.join(WIKIDATA_DIR, "ontology"),
        load_parent_closure: bool = False,
    ) -> Dict[str, "WDClass"]:
        records = deserialize_jl(os.path.join(indir, "classes.jl"))
        records = [WDClass.from_dict(c) for c in records]
        records = {r.id: r for r in records}

        if load_parent_closure:
            logger.info("Load parent closure...")
            parents_closure = deserialize_jl(
                os.path.join(indir, "superclasses_closure.jl")
            )
            for rid, parents in parents_closure:
                records[rid].parents_closure = set(parents)

        return records

    @staticmethod
    def iter_file(
        indir: Union[str, Path] = os.path.join(WIKIDATA_DIR, "ontology"),
        load_parent_closure: bool = False,
    ) -> Generator["WDClass", None, None]:
        if load_parent_closure:
            with open(os.path.join(indir, "classes.jl"), "r") as f, open(
                os.path.join(indir, "superclasses_closure.jl"), "r"
            ) as g:
                while True:
                    try:
                        record = orjson.loads(next(f))
                    except StopIteration:
                        try:
                            next(g)
                        except StopIteration:
                            break
                        raise Exception(
                            "Invalid superclasses_closure file. Number of rows doesn't match with the classes file"
                        )

                    cid, parents = orjson.loads(next(g))
                    assert record["id"] == cid
                    record["parents_closure"] = parents
                    yield WDClass.from_dict(record)
        else:
            with open(os.path.join(indir, "classes.jl"), "r") as f:
                for line in f:
                    yield WDClass.from_dict(orjson.loads(line))

    @staticmethod
    def deserialize(s):
        o = orjson.loads(s)
        return WDClass.from_dict(o)

    @staticmethod
    def from_dict(o):
        o["label"] = MultiLingualString(**o["label"])
        o["description"] = MultiLingualString(**o["description"])
        o["aliases"] = MultiLingualStringList(**o["aliases"])
        o["parents_closure"] = set(o["parents_closure"])
        return WDClass(**o)

    @staticmethod
    def from_qnode(qnode: QNode):
        return WDClass(
            id=qnode.id,
            label=qnode.label,
            description=qnode.description,
            datatype=qnode.datatype,  # type: ignore
            aliases=qnode.aliases,
            parents=sorted(
                {stmt.value.as_entity_id() for stmt in qnode.props.get("P279", [])}
            ),
            properties=sorted(
                {stmt.value.as_entity_id() for stmt in qnode.props.get("P1963", [])}
            ),
            different_froms=sorted(
                {stmt.value.as_entity_id() for stmt in qnode.props.get("P1889", [])}
            ),
            equivalent_classes=sorted(
                {stmt.value.as_string() for stmt in qnode.props.get("P1709", [])}
            ),
            parents_closure=set(),
        )

    def serialize(self):
        odict = {
            k: getattr(self, k)
            for k in [
                "id",
                "label",
                "description",
                "datatype",
                "aliases",
                "parents",
                "properties",
                "different_froms",
                "equivalent_classes",
                "parents_closure",
            ]
        }
        for k in ["label", "description", "aliases"]:
            odict[k] = odict[k].serialize()
        return orjson.dumps(odict, option=orjson.OPT_SERIALIZE_DATACLASS, default=list)

    def get_uri(self):
        return f"http://www.wikidata.org/entity/{self.id}"
