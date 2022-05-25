from typing import Dict, List, Set, Tuple, Union

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


def property_domains(lang="en") -> Dataset[Tuple[str, Dict[str, int]]]:
    """Extract the domains of a property.

    NOTE: it does not returns children of a domain class but only the class that appears
    in the statement with the property.

    For example, consider the statement Peter - age - 50. the direct domain is the class Human, and we don't include
    class Men, which is a child of the class Human.
    """
    cfg = WDDataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.property_domains):
        (
            entities(lang)
            .get_rdd()
            .flatMap(get_property_domains)
            .reduceByKey(merge_counters)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.property_domains),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.property_domains / "*.gz", deserialize=orjson.loads)


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

    lst = []
    for prop in ent.props.keys():
        if prop in ignored_props:
            continue
        lst.append((prop, domains))
    return lst
