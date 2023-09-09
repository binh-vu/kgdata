from typing import Dict, List, Tuple

import orjson

from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity


def property_domains() -> Dataset[Tuple[str, Dict[str, int]]]:
    """Extract the domains of a property.

    NOTE: it does not returns children of a domain class but only the class that appears
    in the statement with the property.

    For example, consider the statement Peter - age - 50. the direct domain is the class Human, and we don't include
    class Men, which is a child of the class Human.
    """
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        cfg.property_domains / "*.gz",
        deserialize=orjson.loads,
        name="property-domains",
        dependencies=[entities()],
    )
    if not ds.has_complete_data():
        (
            entities()
            .get_extended_rdd()
            .flatMap(get_property_domains)
            .reduceByKey(merge_counters)
            .map(orjson.dumps)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def merge_counters(a: Dict[str, int], b: Dict[str, int]):
    out = a.copy()
    for k, v in b.items():
        if k not in out:
            out[k] = 0
        out[k] += v
    return out


def get_property_domains(ent: WDEntity) -> List[Tuple[str, Dict[str, int]]]:
    instanceof = "P31"
    subclass_of = "P279"
    subproperty_of = "P1647"

    ignored_props = {instanceof, subclass_of, subproperty_of}
    domains = {
        stmt.value.as_entity_id_safe(): 1 for stmt in ent.props.get(instanceof, [])
    }

    lst = {}
    for prop, stmts in ent.props.items():
        if prop in ignored_props:
            continue
        lst[prop] = domains
        for stmt in stmts:
            for qid in stmt.qualifiers.keys():
                lst[qid] = domains

    return list(lst.items())
