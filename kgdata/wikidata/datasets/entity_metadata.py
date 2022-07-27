import os
from functools import partial
from pathlib import Path
from typing import Union, cast, Set, Dict

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entities import deser_entity, entities, ser_entity
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


def entity_metadata(lang: str = "en") -> Dataset[WDEntity]:
    """Keep all data of the entities but its properties (set it to empty dictionary)."""

    cfg = WDDataDirCfg.get_instance()
    outdir = cfg.entity_metadata.parent / (cfg.entity_metadata.name + "_" + lang)

    if not does_result_dir_exist(outdir):
        (
            entities()
            .get_rdd()
            .map(empty_ent_props)
            .map(ser_entity)
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(file_pattern=outdir / "*.gz", deserialize=deser_entity)


def empty_ent_props(ent: WDEntity) -> WDEntity:
    ent.props = {}
    return ent
