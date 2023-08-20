from functools import partial
from typing import List

import orjson

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.misc.hierarchy import build_ancestors
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.splitter import split_a_list
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.models import WDProperty
from kgdata.wikidata.models.wdentity import WDEntity


def properties(lang="en") -> Dataset[WDProperty]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.properties / "ids"):
        (
            entities(lang)
            .get_rdd()
            .flatMap(get_property_ids)
            .distinct()
            .coalesce(128, shuffle=True)
            .saveAsTextFile(
                str(cfg.properties / "ids"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.properties / "properties"):
        sc = get_spark_context()
        prop_ids = sc.broadcast(
            set(sc.textFile(str(cfg.properties / "ids/*.gz")).collect())
        )
        (
            entities(lang)
            .get_rdd()
            .filter(lambda ent: ent.id in prop_ids.value)
            .map(lambda x: WDProperty.from_entity(x).to_dict())
            .map(orjson.dumps)
            .coalesce(128, shuffle=True)
            .saveAsTextFile(
                str(cfg.properties / "properties"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not (cfg.properties / "unknown_properties.txt").exists():
        saveAsSingleTextFile(
            get_spark_context()
            .textFile(str(cfg.properties / "ids/*.gz"))
            .subtract(entity_ids().get_rdd())
            .subtract(entity_redirections().get_rdd().map(lambda x: x[0])),
            cfg.properties / "unknown_properties.txt",
        )

    get_ds = lambda subdir: Dataset(
        cfg.properties / subdir / "*.gz",
        deserialize=partial(deser_from_dict, WDProperty),
    )

    if not does_result_dir_exist(cfg.properties / "full_properties"):
        properties = get_ds("properties").get_list()
        build_ancestors(properties)
        split_a_list(
            [ser_to_dict(p) for p in properties],
            cfg.properties / "full_properties" / "part.jl.gz",
        )
        (cfg.properties / "full_properties" / "_SUCCESS").touch()

    return get_ds("full_properties")


def get_property_ids(ent: WDEntity) -> List[str]:
    prop_ids = set()
    if ent.type == "property":
        prop_ids.add(ent.id)

    # P1647: subpropertyof
    for stmt in ent.props.get("P1647", []):
        if stmt.value.is_entity_id(stmt.value):
            prop_ids.add(stmt.value.as_entity_id())

    # statement property and qualifiers
    prop_ids.update(ent.props.keys())
    for stmts in ent.props.values():
        for stmt in stmts:
            prop_ids.update(stmt.qualifiers.keys())

    return list(prop_ids)
