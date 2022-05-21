import gzip
from kgdata.dataset import Dataset
import orjson
from collections import deque
from typing import List, Dict
import sm.misc as M
from tqdm import tqdm
from kgdata.wikidata.models import WDClass
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entities import entities, ser_entity
from kgdata.splitter import split_a_list
from kgdata.wikidata.models.wdentity import WDEntity


def classes(lang: str = "en") -> Dataset[WDClass]:
    cfg = WDDataDirCfg.get_instance()

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

    if not does_result_dir_exist(cfg.classes / "classes"):
        sc = get_spark_context()
        class_ids = sc.broadcast(
            set(sc.textFile(str(cfg.classes / "ids/*.gz")).collect())
        )
        (
            entities(lang)
            .get_rdd()
            .filter(lambda ent: ent.id in class_ids.value)
            .map(lambda x: orjson.dumps(extract_class(x).to_dict()))
            .saveAsTextFile(
                str(cfg.classes / "classes"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.classes / "ancestors"):
        sc = get_spark_context()
        saveAsSingleTextFile(
            sc.textFile(str(cfg.classes / "classes/*.gz"))
            .map(orjson.loads)
            .map(WDClass.from_dict)
            .map(lambda x: (x.id, x.parents))
            .map(orjson.dumps),
            str(cfg.classes / "ancestors/id2parents.ndjson.gz"),
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

        id2parents = {
            k: v
            for k, v in M.deserialize_jl(cfg.classes / "ancestors/id2parents.ndjson.gz")
        }

        id2ancestors = build_ancestors(id2parents)
        split_a_list(
            [orjson.dumps(x) for x in sorted(id2ancestors.items())],
            (cfg.classes / "ancestors/id2ancestors/part.ndjson.gz"),
        )
        (cfg.classes / "ancestors" / "_SUCCESS").touch()

    if not does_result_dir_exist(cfg.classes / "full_classes"):
        sc = get_spark_context()
        id2ancestors = sc.textFile(
            str(cfg.classes / "ancestors/id2ancestors/*.gz")
        ).map(orjson.loads)

        def merge_ancestors(o):
            id, (cls, ancestors) = o
            cls.ancestors = set(ancestors)
            return cls

        (
            sc.textFile(str(cfg.classes / "classes/*.gz"))
            .map(orjson.loads)
            .map(WDClass.from_dict)
            .map(lambda x: (x.id, x))
            .join(id2ancestors)
            .map(merge_ancestors)
            .map(WDClass.to_dict)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.classes / "full_classes"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        cfg.classes / "full_classes/*.gz",
        deserialize=lambda x: WDClass.from_dict(orjson.loads(x)),
    )


def extract_class(ent: WDEntity) -> WDClass:
    cls = WDClass.from_entity(ent)
    # we do have cases where the class is a subclass of itself, which is wrong.
    cls.parents = [p for p in cls.parents if p != cls.id]
    return cls


def build_ancestors(id2parents: dict) -> dict:
    id2ancestors = {}
    for id in tqdm(id2parents, desc="build ancestors"):
        id2ancestors[id] = get_ancestors(id, id2parents)
    return id2ancestors


def get_ancestors(id: str, id2parents: dict) -> List[str]:
    # preserved the order
    ancestors = {}
    queue = deque(id2parents[id])
    while len(queue) > 0:
        ptr = queue.popleft()
        if ptr in ancestors:
            continue

        ancestors[ptr] = len(ancestors)
        queue.extend(id2parents[ptr])

    return list(ancestors.keys())


def get_class_ids(ent: WDEntity) -> List[str]:
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
