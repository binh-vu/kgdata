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
        entity_rdd = entities(lang).get_rdd()
        (
            entity_rdd.flatMap(get_target_property_entity)
            .join(entity_rdd.flatMap(get_instanceof))
            .map(lambda x: (x[1][0], {cls_id: 1 for cls_id in x[1][1]}))
            .reduceByKey(merge_counters)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.property_ranges),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.property_ranges / "*.gz", deserialize=orjson.loads)


def get_instanceof(ent: WDEntity) -> List[Tuple[str, str]]:
    instanceof = "P31"
    return [
        (ent.id, stmt.value.as_entity_id_safe())
        for stmt in ent.props.get(instanceof, [])
    ]


def get_target_property_entity(ent: WDEntity) -> List[Tuple[str, str]]:
    instanceof = "P31"
    subclass_of = "P279"
    subproperty_of = "P1647"

    ignored_props = {instanceof, subclass_of, subproperty_of}

    out = set()
    for pid, stmts in ent.props.items():
        if pid in ignored_props:
            continue

        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                out.add((stmt.value.as_entity_id(), pid))

            for qid, qvals in stmt.qualifiers.items():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        out.add((qval.as_entity_id(), qid))

    return list(out)
