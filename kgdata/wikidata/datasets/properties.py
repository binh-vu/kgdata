from kgdata.dataset import Dataset
from kgdata.splitter import split_a_list
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
import orjson
from typing import List
from kgdata.wikidata.models import WDProperty
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entities import entities, ser_entity
from kgdata.wikidata.datasets.classes import build_ancestors
import sm.misc as M
from kgdata.wikidata.models.wdentity import WDEntity


def properties(lang="en") -> Dataset[WDProperty]:
    cfg = WDDataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.properties / "ids"):
        (
            entities(lang)
            .get_rdd()
            .flatMap(get_property_ids)
            .distinct()
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

    if not does_result_dir_exist(cfg.properties / "ancestors"):
        sc = get_spark_context()

        saveAsSingleTextFile(
            sc.textFile(str(cfg.properties / "properties/*.gz"))
            .map(orjson.loads)
            .map(WDProperty.from_dict)
            .map(lambda x: (x.id, x.parents))
            .map(orjson.dumps),
            str(cfg.properties / "ancestors/id2parents.ndjson.gz"),
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

        id2parents = {
            k: v
            for k, v in M.deserialize_jl(
                cfg.properties / "ancestors/id2parents.ndjson.gz"
            )
        }

        id2ancestors = build_ancestors(id2parents)
        split_a_list(
            [orjson.dumps(x) for x in sorted(id2ancestors.items())],
            (cfg.properties / "ancestors/id2ancestors/part.ndjson.gz"),
        )
        (cfg.properties / "ancestors" / "_SUCCESS").touch()

    if not does_result_dir_exist(cfg.properties / "full_properties"):
        sc = get_spark_context()
        id2ancestors = sc.textFile(
            str(cfg.properties / "ancestors/id2ancestors/*.gz")
        ).map(orjson.loads)

        def merge_ancestors(o):
            id, (prop, ancestors) = o
            prop.ancestors = set(ancestors)
            return prop

        (
            sc.textFile(str(cfg.properties / "properties/*.gz"))
            .map(orjson.loads)
            .map(WDProperty.from_dict)
            .map(lambda x: (x.id, x))
            .join(id2ancestors)
            .map(merge_ancestors)
            .map(WDProperty.to_dict)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.properties / "full_properties"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        cfg.properties / "full_properties/*.gz",
        deserialize=lambda x: WDProperty.from_dict(orjson.loads(x)),
    )


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

