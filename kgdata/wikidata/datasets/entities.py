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
import orjson
import sm.misc as M
from pyspark.rdd import RDD
from pyspark import Broadcast
from kgdata.dataset import Dataset


def entities(lang: str = "en") -> Dataset[WDEntity]:
    """Normalize Wikidata entity from Wikidata entity json dumps.

    This data does not verify if references within entities are valid. For example,
    an entity may have a property value that links to another entity that does not exist.

    Returns:
        Dataset[WDEntity]
    """
    cfg = WDDataDirCfg.get_instance()
    outdir = cfg.entities.parent / (cfg.entities.name + "_" + lang)

    if not does_result_dir_exist(outdir / "unverified"):
        (
            entity_dump()
            .get_rdd()
            .map(partial(WDEntity.from_wikidump, lang=lang))
            .map(ser_entity)
            .saveAsTextFile(
                str(outdir / "unverified"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(outdir / "all_ids"):
        (
            get_spark_context()
            .textFile(str(outdir / "unverified/*.gz"))
            .map(deser_entity)
            .flatMap(get_child_entities)
            .distinct()
            .saveAsTextFile(
                str(outdir / "all_ids"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )
    all_ids = get_spark_context().textFile(str(outdir / "all_ids/*.gz"))

    if not (outdir / "unknown_entities.txt").exists():
        saveAsSingleTextFile(
            all_ids.subtract(entity_ids()).subtract(
                cast(RDD, entity_redirections()).map(lambda x: x[0])
            ),
            outdir / "unknown_entities.txt",
        )

    if not (outdir / "redirected_entities.tsv").exists():
        redirect_rdd = entity_redirections()
        assert isinstance(redirect_rdd, RDD)

        saveAsSingleTextFile(
            all_ids.map(lambda x: (x, 1))
            .join(redirect_rdd)
            .map(lambda x: (x[0], x[1][1]))
            .map(lambda x: "\t".join(x)),
            outdir / "redirected_entities.tsv",
        )

    if not does_result_dir_exist(outdir / "fixed"):
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

        # tmp = (
        #     sc.textFile(str(outdir / "unverified/*.gz"))
        #     .map(deser_entity)
        #     .map(lambda x: fixed_entity(x, unknown_entities, redirected_entities))
        #     .filter(lambda x: x[1])
        #     .cache()
        # )
        # print(tmp.count())
        # print(tmp.map(lambda x: x[0].id).take(10))

        (
            sc.textFile(str(outdir / "unverified/*.gz"))
            .map(deser_entity)
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
    children = set()
    for stmts in ent.props.values():
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                children.add(stmt.value.as_entity_id())

            for qvals in stmt.qualifiers.values():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        children.add(qval.as_entity_id())
    return list(children)


def fixed_entity(
    ent: WDEntity, bc_unknown_entities: Broadcast, bc_redirected_entities: Broadcast
):
    unknown_entities: Set[str] = bc_unknown_entities.value
    redirected_entities: Dict[str, str] = bc_redirected_entities.value

    is_fixed = False

    for pid, stmts in list(ent.props.items()):
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
                elif len(removed_qualifiers) > 0:
                    stmt.qualifiers[qid] = [
                        qval
                        for j, qval in enumerate(qvals)
                        if j not in removed_qualifiers
                    ]

        is_fixed = is_fixed or len(removed_stmts) > 0
        if len(removed_stmts) == len(stmts):
            del ent.props[pid]
        elif len(removed_stmts) > 0:
            ent.props[pid] = [
                stmt for i, stmt in enumerate(stmts) if i not in removed_stmts
            ]

    return ent, is_fixed


if __name__ == "__main__":
    WDDataDirCfg.init("/data/binhvu/sm-dev/data/wikidata/20211213")
    print("Total:", entities().get_rdd().count())
