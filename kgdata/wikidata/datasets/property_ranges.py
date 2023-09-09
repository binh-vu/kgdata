from collections import defaultdict
from typing import Dict, List, Tuple

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.datasets.property_domains import merge_counters
from kgdata.wikidata.models.wdentity import WDEntity


def property_ranges() -> Dataset[Tuple[str, Dict[str, int]]]:
    """Extract the ranges of a property.

    NOTE: it does not returns children of a range class but only the class that is typed of an entity
    in the statement target with the property.
    """
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        cfg.property_ranges / "*.gz",
        deserialize=orjson.loads,
        name="property-ranges",
        dependencies=[entities(), entity_types()],
    )

    if not ds.has_complete_data():
        # mapping from entity id to the incoming properties with counts
        (
            entities()
            .get_extended_rdd()
            .flatMap(get_target_property_entity)
            .reduceByKey(merge_counters)
            .join(entity_types().get_extended_rdd())
            .flatMap(lambda x: join_prop_counts_and_types(x[1][0], x[1][1]))
            .reduceByKey(merge_counters)
            .map(orjson.dumps)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def join_prop_counts_and_types(
    prop_counts: Dict[str, int], classes: List[str]
) -> List[Tuple[str, Dict[str, int]]]:
    out = []
    for prop, count in prop_counts.items():
        out.append((prop, {cls: count for cls in classes}))
    return out


def get_target_property_entity(ent: WDEntity) -> List[Tuple[str, Dict[str, int]]]:
    instanceof = "P31"
    subclass_of = "P279"
    subproperty_of = "P1647"

    ignored_props = {instanceof, subclass_of, subproperty_of}

    out = defaultdict(set)
    for pid, stmts in ent.props.items():
        if pid in ignored_props:
            continue

        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                out[stmt.value.as_entity_id()].add(pid)

            for qid, qvals in stmt.qualifiers.items():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        out[qval.as_entity_id()].add(qid)

    return [(ent_id, {pid: 1 for pid in props}) for ent_id, props in out.items()]
