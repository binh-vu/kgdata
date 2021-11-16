import glob
import gzip
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Set, Union

import orjson

from kgdata.config import WIKIDATA_DIR
from kgdata.wikidata.models.qnode import (
    QNode,
    MultiLingualString,
    MultiLingualStringList,
)
from sm.misc.deser import deserialize_jl, deserialize_json


@dataclass
class WDProperty:
    id: str
    label: MultiLingualString
    description: MultiLingualString
    # wikibase-lexeme, monolingualtext, wikibase-sense, url, wikibase-property,
    # wikibase-form, external-id, time, commonsMedia, quantity, wikibase-item, musical-notation,
    # tabular-data, string, math, geo-shape, globe-coordinate
    datatype: str
    aliases: MultiLingualStringList
    parents: List[str]
    see_also: List[str]
    equivalent_properties: List[str]
    subjects: List[str]
    inverse_properties: List[str]
    instanceof: List[str]
    parents_closure: Set[str] = field(default_factory=set)

    @staticmethod
    def from_file(
        indir: Union[str, Path] = os.path.join(WIKIDATA_DIR, "ontology"),
        load_parent_closure: bool = False,
    ) -> Dict[str, "WDProperty"]:
        records = deserialize_jl(os.path.join(indir, "properties.jl"))
        records = [WDProperty.from_dict(c) for c in records]
        records = {r.id: r for r in records}

        if load_parent_closure:
            parents_closure = deserialize_jl(
                os.path.join(indir, "superproperties_closure.jl")
            )
            for rid, parents in parents_closure:
                records[rid].parents_closure = set(parents)

        return records

    @staticmethod
    def deserialize(s):
        o = orjson.loads(s)
        return WDProperty.from_dict(o)

    @staticmethod
    def from_dict(o):
        o["label"] = MultiLingualString(**o["label"])
        o["description"] = MultiLingualString(**o["description"])
        o["aliases"] = MultiLingualStringList(**o["aliases"])
        o["parents_closure"] = set(o["parents_closure"])
        return WDProperty(**o)

    @staticmethod
    def from_qnode(qnode: QNode):
        try:
            return WDProperty(
                id=qnode.id,
                label=qnode.label,
                description=qnode.description,
                datatype=qnode.datatype,  # type: ignore
                aliases=qnode.aliases,
                parents=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P279", [])}
                ),
                see_also=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1659", [])}
                ),
                equivalent_properties=sorted(
                    {stmt.value.as_string() for stmt in qnode.props.get("P1628", [])}
                ),
                subjects=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1629", [])}
                ),
                inverse_properties=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1696", [])}
                ),
                instanceof=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P31", [])}
                ),
            )
        except:
            print(qnode)
            raise

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
                "see_also",
                "equivalent_properties",
                "subjects",
                "inverse_properties",
                "instanceof",
                "parents_closure",
            ]
        }
        for k in ["label", "description", "aliases"]:
            odict[k] = odict[k].serialize()
        return orjson.dumps(odict, option=orjson.OPT_SERIALIZE_DATACLASS, default=list)

    def get_uri(self):
        return f"http://www.wikidata.org/prop/{self.id}"

    def is_object_property(self):
        return self.datatype == "wikibase-item"

    def is_data_property(self):
        return not self.is_object_property()

    def is_transitive(self):
        return "Q18647515" in self.instanceof


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
