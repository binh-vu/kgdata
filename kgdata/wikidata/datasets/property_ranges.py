from collections import defaultdict
from typing import Dict, List, Set, Tuple, Union
from kgdata.wikidata.datasets.property_domains import merge_counters

import orjson
import sm.misc as M
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.splitter import split_a_list
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.classes import build_ancestors
from kgdata.wikidata.datasets.entities import entities, ser_entity
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.models import WDProperty
from kgdata.wikidata.models.wdentity import WDEntity


def property_ranges(lang="en") -> Dataset[Tuple[str, Dict[str, int]]]:
    """Extract the ranges of a property.

    NOTE: it does not returns children of a range class but only the class that is typed of an entity
    in the statement target with the property.
    """
    cfg = WDDataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.property_ranges):
        # mapping from entity id to the incoming properties with counts
        ent_prop_counts = (
            entities(lang)
            .get_rdd()
            .flatMap(get_target_property_entity)
            .reduceByKey(merge_counters)
        )
        (
            ent_prop_counts.join(entity_types(lang).get_rdd())
            .flatMap(lambda x: join_prop_counts_and_types(x[1][0], x[1][1]))
            .reduceByKey(merge_counters)
            .coalesce(256)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.property_ranges),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.property_ranges / "*.gz", deserialize=orjson.loads)


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
