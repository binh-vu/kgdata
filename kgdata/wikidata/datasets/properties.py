import orjson
from typing import List
from kgdata.wikidata.models.new_wdprop import WDProperty
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entities import entities, ser_entity
from kgdata.wikidata.datasets.classes import build_ancestors
import sm.misc as M
from kgdata.wikidata.models.wdentity import WDEntity


def properties():
    cfg = WDDataDirCfg.get_instance()
    sc = get_spark_context()

    if not does_result_dir_exist(cfg.properties / "ids"):
        (
            entities()
            .flatMap(get_property_ids)
            .distinct()
            .saveAsTextFile(
                str(cfg.properties / "ids"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.properties / "properties"):
        prop_ids = sc.broadcast(
            set(sc.textFile(str(cfg.properties / "ids/*.gz")).collect())
        )
        (
            entities()
            .filter(lambda ent: ent.id in prop_ids.value)
            .map(lambda x: WDProperty.from_entity(x).to_dict())
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.properties / "properties"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.properties / "ancestors"):
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
        (cfg.properties / "ancestors" / "_SUCCESS").touch()

        id2ancestors = build_ancestors(id2parents)
        M.serialize_jl(
            sorted(id2ancestors.items()),
            cfg.properties / "ancestors/id2ancestors.ndjson.gz",
        )


def get_property_ids(ent: WDEntity) -> List[str]:
    prop_ids = set()
    if ent.type == "property":
        prop_ids.add(ent.id)

    # P1647: subpropertyof
    for stmt in ent.props.get("P1647", []):
        if stmt.value.is_entity_id(stmt.value):
            prop_ids.add(stmt.value.as_entity_id())

    for stmts in ent.props.values():
        for stmt in stmts:
            prop_ids.update(stmt.qualifiers.keys())
    prop_ids.update(ent.props.keys())
    return list(prop_ids)
