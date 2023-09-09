from __future__ import annotations

import shutil
from functools import partial

import orjson

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.misc.hierarchy import build_ancestors
from kgdata.misc.modification import Modification
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_list
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models import WDClass
from kgdata.wikidata.models.wdentity import WDEntity


def classes(lang: str = "en") -> Dataset[WDClass]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.classes / "ids"):
        (
            entities(lang)
            .get_rdd()
            .flatMap(get_class_ids)
            .distinct()
            .saveAsTextFile(
                str(cfg.classes / "ids"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    get_ds = lambda subdir: Dataset(
        cfg.classes / f"{subdir}-{lang}/*.gz",
        deserialize=partial(deser_from_dict, WDClass),
        name=f"classes/{subdir}/{lang}",
        dependencies=[entities(lang)],
    )
    basic_ds = get_ds("basic")
    full_ds = get_ds("full")

    if not basic_ds.has_complete_data():
        sc = get_spark_context()
        class_ids = sc.broadcast(
            set(sc.textFile(str(cfg.classes / "ids/*.gz")).collect())
        )
        (
            entities(lang)
            .get_extended_rdd()
            .filter(lambda ent: ent.id in class_ids.value)
            .map(lambda x: orjson.dumps(extract_class(x).to_dict()))
            .save_like_dataset(basic_ds, auto_coalesce=True, checksum=False)
        )

    if not full_ds.has_complete_data():
        classes = basic_ds.get_list()

        # fix the class based on manual modification -- even if there is no modification
        # the file should be there to prevent typo
        assert (cfg.modification / "classes.tsv").exists()
        shutil.copy2(
            cfg.modification / "classes.tsv", cfg.classes / "classes.modified.tsv"
        )
        id2class = {c.id: c for c in classes}
        id2mods = Modification.from_tsv(cfg.classes / "classes.modified.tsv")
        for cid, mods in id2mods.items():
            for mod in mods:
                mod.apply(id2class[cid])

        build_ancestors(classes)
        split_a_list(
            [ser_to_dict(c) for c in classes],
            full_ds.get_data_directory() / "part.jl.gz",
        )
        full_ds.sign(full_ds.get_name(), full_ds.get_dependencies())

    return full_ds


def extract_class(ent: WDEntity) -> WDClass:
    cls = WDClass.from_entity(ent)
    # we do have cases where the class is a subclass of itself, which is wrong.
    cls.parents = [p for p in cls.parents if p != cls.id]
    return cls


def get_class_ids(ent: WDEntity) -> list[str]:
    # we can have a case where a property is mistakenly marked as a class such as P1072, which has been fixed.
    # but we need to check it here.
    if ent.id[0] == "P":
        return []

    lst = set()
    if "P279" in ent.props:
        lst.add(ent.id)

    # P279: subclassof -- according to wikidata, this is the only property to detect class
    # P31: instanceof -- sometimes entity that is not a class is value of this property (probably human-error)
    for pid in ["P279", "P31"]:
        for stmt in ent.props.get(pid, []):
            if stmt.value.is_entity_id(stmt.value):
                lst.add(stmt.value.as_entity_id())

    # class of class:
    # - Wikidata metaclass Q19361238
    # - metaclass Q19478619
    # - class Q5127848
    if any(
        stmt.value.as_entity_id() in {"Q19361238", "Q19478619", "Q5127848"}
        for stmt in ent.props.get("P31", [])
        if stmt.value.is_entity_id(stmt.value)
    ):
        lst.add(ent.id)

    return list(lst)
