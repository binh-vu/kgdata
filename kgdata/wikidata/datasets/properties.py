import shutil
from functools import partial
from typing import List

import orjson

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.misc.hierarchy import build_ancestors
from kgdata.misc.modification import Modification
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.spark.extended_rdd import ExtendedRDD
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

    get_ds = lambda subdir: Dataset(
        cfg.properties / f"{subdir}-{lang}/*.gz",
        deserialize=partial(deser_from_dict, WDProperty),
        name=f"properties/{subdir}/{lang}",
        dependencies=[entities(lang)],
    )
    basic_ds = get_ds("basic")
    full_ds = get_ds("full")

    if not basic_ds.has_complete_data():
        sc = get_spark_context()
        prop_ids = sc.broadcast(
            set(sc.textFile(str(cfg.properties / "ids/*.gz")).collect())
        )
        (
            entities(lang)
            .get_extended_rdd()
            .filter(lambda ent: ent.id in prop_ids.value)
            .map(lambda x: WDProperty.from_entity(x).to_dict())
            .map(orjson.dumps)
            .coalesce(128, shuffle=True)
            .save_like_dataset(
                basic_ds,
                auto_coalesce=True,
                shuffle=True,
            )
        )

    if not (cfg.properties / "unknown_properties.txt").exists():
        (
            ExtendedRDD.textFile(str(cfg.properties / "ids/*.gz"))
            .subtract(entity_ids().get_extended_rdd())
            .subtract(entity_redirections().get_extended_rdd().map(lambda x: x[0]))
            .save_as_single_text_file(cfg.properties / "unknown_properties.txt")
        )

    if not full_ds.has_complete_data():
        properties = basic_ds.get_list()

        # fix the prop based on manual modification -- even if there is no modification
        # the file should be there to prevent typo
        assert (cfg.modification / "props.tsv").exists()
        shutil.copy2(
            cfg.modification / "props.tsv", cfg.properties / "props.modified.tsv"
        )
        id2prop = {p.id: p for p in properties}
        id2mods = Modification.from_tsv(cfg.properties / "props.modified.tsv")
        for cid, mods in id2mods.items():
            for mod in mods:
                mod.apply(id2prop[cid])

        build_ancestors(properties)
        split_a_list(
            [ser_to_dict(p) for p in properties],
            full_ds.get_data_directory() / "part.jl.gz",
        )
        full_ds.sign(full_ds.get_name(), full_ds.get_dependencies())

    return full_ds


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
