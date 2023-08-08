from __future__ import annotations

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity


def entity_types(lang="en") -> Dataset[tuple[str, list[str]]]:
    """Extract types of entities. Mapping from entity id to its type"""
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.entity_types):
        (
            entities(lang)
            .get_rdd()
            .map(get_instanceof)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.entity_types),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.entity_types / "*.gz", deserialize=orjson.loads)


def get_instanceof(ent: WDEntity) -> tuple[str, list[str]]:
    instanceof = "P31"
    return (
        ent.id,
        list(
            {stmt.value.as_entity_id_safe() for stmt in ent.props.get(instanceof, [])}
        ),
    )
