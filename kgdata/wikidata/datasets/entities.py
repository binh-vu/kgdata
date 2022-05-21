import os
from functools import partial
from pathlib import Path
from typing import Union, cast, Set, Dict

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dump
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.models.wdentity import WDEntity, WDValue
from loguru import logger
import orjson
import sm.misc as M
from pyspark.rdd import RDD
from pyspark import Broadcast
from kgdata.dataset import Dataset


def entities(lang: str = "en") -> Dataset[WDEntity]:
    """Normalize Wikidata entity from Wikidata entity json dumps.

    In the json dumps, an entity can linked to an entity that either:
    1. in the dump
    2. not in the dump but in a redirection list
    3. not in the dump and not in a redirection list (e.g., deleted entity or lexeme that is not included in the dump).

    Similarly, property of an entity can also not in the dump.

    This dataset fixed all the issues mentioned above by resolving redirection and removed unknown entities (including properties).

    Returns:
        Dataset[WDEntity]
    """
    cfg = WDDataDirCfg.get_instance()
    outdir = cfg.entities.parent / (cfg.entities.name + "_" + lang)

    if not does_result_dir_exist(outdir / "all_ids"):
        logger.info("Getting all entity ids in the dump (including properties)")
        (
            entity_dump()
            .get_rdd()
            .map(partial(WDEntity.from_wikidump, lang=lang))
            .flatMap(get_child_entities)
            .distinct()
            .saveAsTextFile(
                str(outdir / "all_ids"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not (outdir / "unknown_entities.txt").exists():
        logger.info("Identifying unknown entities")
        saveAsSingleTextFile(
            get_spark_context()
            .textFile(str(outdir / "all_ids/*.gz"))
            .subtract(entity_ids().get_rdd())
            .subtract(entity_redirections().get_rdd().map(lambda x: x[0])),
            outdir / "unknown_entities.txt",
        )

    if not (outdir / "redirected_entities.tsv").exists():
        logger.info("Identifying redirected entities")
        saveAsSingleTextFile(
            get_spark_context()
            .textFile(str(outdir / "all_ids/*.gz"))
            .map(lambda x: (x, 1))
            .join(entity_redirections().get_rdd())
            .map(lambda x: (x[0], x[1][1]))
            .map(lambda x: "\t".join(x)),
            outdir / "redirected_entities.tsv",
        )

    if not does_result_dir_exist(outdir / "fixed"):
        logger.info("Normalizing and fixing the entities in the dump")

        sc = get_spark_context()
        unknown_entities = sc.broadcast(
            set(M.deserialize_lines(outdir / "unknown_entities.txt", trim=True))
        )
        redirected_entities = sc.broadcast(
            {
                k: v
                for k, v in (
                    line.split("\t")
                    for line in M.deserialize_lines(
                        outdir / "redirected_entities.tsv", trim=True
                    )
                )
            }
        )

        (
            entity_dump()
            .get_rdd()
            .map(partial(WDEntity.from_wikidump, lang=lang))
            .map(lambda x: fixed_entity(x, unknown_entities, redirected_entities))
            .map(lambda x: x[0])
            .map(ser_entity)
            .saveAsTextFile(
                str(outdir / "fixed"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(file_pattern=outdir / "fixed/*.gz", deserialize=deser_entity)


def deser_entity(line: Union[str, bytes]) -> WDEntity:
    return WDEntity.from_dict(orjson.loads(line))


def ser_entity(ent: WDEntity):
    return orjson.dumps(ent.to_dict())


def get_child_entities(ent: WDEntity):
    """Get entities that are children of an entity and properties/qualifiers used by the entity."""
    children = set()
    for pid, stmts in ent.props.items():
        children.add(pid)
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                children.add(stmt.value.as_entity_id())

            for qid, qvals in stmt.qualifiers.items():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        children.add(qval.as_entity_id())
                children.add(qid)
    return list(children)


def fixed_entity(
    ent: WDEntity, bc_unknown_entities: Broadcast, bc_redirected_entities: Broadcast
):
    unknown_entities: Set[str] = bc_unknown_entities.value
    redirected_entities: Dict[str, str] = bc_redirected_entities.value

    is_fixed = False
    for pid, stmts in list(ent.props.items()):
        if pid in unknown_entities:
            del ent.props[pid]
            is_fixed = True
            continue

        removed_stmts = set()
        for i, stmt in enumerate(stmts):
            if stmt.value.is_entity_id(stmt.value):
                ent_id = stmt.value.as_entity_id()
                if ent_id in unknown_entities:
                    removed_stmts.add(i)
                    continue
                if ent_id in redirected_entities:
                    stmt.value.value["id"] = redirected_entities[ent_id]
                    is_fixed = True

            for qid, qvals in list(stmt.qualifiers.items()):
                if qid in unknown_entities:
                    del stmt.qualifiers[qid]
                    stmt.qualifiers_order = [
                        x for x in stmt.qualifiers_order if x != qid
                    ]
                    is_fixed = True
                    continue

                removed_qualifiers = set()
                for j, qval in enumerate(qvals):
                    if qval.is_entity_id(qval):
                        ent_id = qval.as_entity_id()
                        if ent_id in unknown_entities:
                            removed_qualifiers.add(j)
                            continue

                        if ent_id in redirected_entities:
                            qval.value["id"] = redirected_entities[ent_id]

                is_fixed = is_fixed or len(removed_qualifiers) > 0
                if len(removed_qualifiers) == len(qvals):
                    del stmt.qualifiers[qid]
                    stmt.qualifiers_order = [
                        x for x in stmt.qualifiers_order if x != qid
                    ]
                    continue

                if len(removed_qualifiers) > 0:
                    stmt.qualifiers[qid] = [
                        qval
                        for j, qval in enumerate(qvals)
                        if j not in removed_qualifiers
                    ]

                if qid in redirected_entities:
                    is_fixed = True
                    stmt.qualifiers[redirected_entities[qid]] = stmt.qualifiers.pop(qid)
                    stmt.qualifiers_order = [
                        x if x != qid else redirected_entities[qid]
                        for x in stmt.qualifiers_order
                    ]

        is_fixed = is_fixed or len(removed_stmts) > 0
        if len(removed_stmts) == len(stmts):
            del ent.props[pid]
            continue

        if len(removed_stmts) > 0:
            ent.props[pid] = [
                stmt for i, stmt in enumerate(stmts) if i not in removed_stmts
            ]

        if pid in redirected_entities:
            is_fixed = True
            ent.props[redirected_entities[pid]] = ent.props.pop(pid)

    return ent, is_fixed


if __name__ == "__main__":
    WDDataDirCfg.init("/data/binhvu/sm-dev/data/wikidata/20211213")
    print("Total:", entities().get_rdd().count())
