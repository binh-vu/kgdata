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
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.models import WDProperty
from kgdata.wikidata.models.wdentity import WDEntity


def entity_types(lang="en") -> Dataset[Tuple[str, List[str]]]:
    """Extract types of entities. Mapping from entity id to its type"""
    cfg = WDDataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.entity_types):
        (
            entities(lang)
            .get_rdd()
            .map(get_instanceof)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.entity_types),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.entity_types / "*.gz", deserialize=orjson.loads)


def get_instanceof(ent: WDEntity) -> Tuple[str, List[str]]:
    instanceof = "P31"
    return (
        ent.id,
        list(
            {stmt.value.as_entity_id_safe() for stmt in ent.props.get(instanceof, [])}
        ),
    )
