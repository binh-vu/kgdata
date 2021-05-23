import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Set

import orjson

from kgdata.config import WIKIDATA_DIR
from kgdata.misc import deserialize_json, deserialize_jl
from kgdata.wikidata.models.qnode import QNode, MultiLingualString, MultiLingualStringList


@dataclass
class WDClass:
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
    parents_closure: Set[str] = field(default_factory=set)

    @staticmethod
    def from_file(indir: str = os.path.join(WIKIDATA_DIR, "ontology"), load_parent_closure: bool = False) -> Dict[
        str, 'WDClass']:
        records = deserialize_jl(os.path.join(indir, "classes.jl"))
        records = [WDClass(**c) for c in records]

        if load_parent_closure:
            parents_closure = deserialize_json(os.path.join(indir, "superclasses_closure.json"))
            for r in records:
                r.parents_closure = set(parents_closure[r.id])

        return {r.id: r for r in records}

    @staticmethod
    def deserialize(s):
        o = orjson.loads(s)
        return WDClass.from_dict(o)

    @staticmethod
    def from_dict(o):
        o['label'] = MultiLingualString(**o['label'])
        o['description'] = MultiLingualString(**o['description'])
        o['aliases'] = MultiLingualStringList(**o['aliases'])
        o['parents_closure'] = set(o['parents_closure'])
        return WDClass(**o)

    @staticmethod
    def from_qnode(qnode: QNode):
        return WDClass(
            id=qnode.id,
            label=qnode.label,
            description=qnode.description,
            datatype=qnode.datatype,
            aliases=qnode.aliases,
            parents=sorted({stmt.value.as_qnode_id() for stmt in qnode.props.get("P279", [])}),
            properties=sorted({stmt.value.as_qnode_id() for stmt in qnode.props.get("P1963", [])}),
            different_froms=sorted({stmt.value.as_qnode_id() for stmt in qnode.props.get("P1889", [])}),
            equivalent_classes=sorted({stmt.value.as_string() for stmt in qnode.props.get("P1709", [])})
        )

    def serialize(self):
        odict = {k: getattr(self, k) for k in
                 ["id", "label", "description", "datatype", "aliases", "parents", "properties", "different_froms", "equivalent_classes", "parents_closure"]}
        for k in ['label', 'description', 'aliases']:
            odict[k] = odict[k].serialize()
        return orjson.dumps(odict,
                            option=orjson.OPT_SERIALIZE_DATACLASS,
                            default=list)

    def get_uri(self):
        return f"http://www.wikidata.org/entity/{self.id}"
