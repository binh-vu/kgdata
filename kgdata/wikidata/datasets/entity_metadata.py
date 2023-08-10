from typing import Union

import orjson

from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitymetadata import WDEntityMetadata


def entity_metadata(lang: str = "en") -> Dataset[WDEntityMetadata]:
    """Keep all data of the entities but its properties (set it to empty dictionary)."""

    cfg = WikidataDirCfg.get_instance()
    outdir = cfg.entity_metadata.parent / (cfg.entity_metadata.name + "_" + lang)

    if not does_result_dir_exist(outdir):
        (
            entities()
            .get_rdd()
            .map(convert_to_entity_metadata)
            .map(ser_entity_metadata)
            .coalesce(1024, shuffle=True)
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(file_pattern=outdir / "*.gz", deserialize=deser_entity_metadata)


def deser_entity_metadata(b: Union[str, bytes]) -> WDEntityMetadata:
    return WDEntityMetadata.from_tuple(orjson.loads(b))


def ser_entity_metadata(ent: WDEntityMetadata) -> bytes:
    return orjson.dumps(ent.to_tuple())


def convert_to_entity_metadata(ent: WDEntity) -> WDEntityMetadata:
    props = {}
    for pid in ["P31", "P279", "P1647"]:
        props[pid] = []
        for stmt in ent.props.get(pid, []):
            if stmt.rank == "deprecated":
                continue
            if stmt.value.is_entity_id(stmt.value):
                props[pid].append(stmt.value.as_entity_id())

    return WDEntityMetadata(
        id=ent.id,
        label=ent.label,
        description=ent.description,
        aliases=ent.aliases,
        instanceof=props["P31"],
        subclassof=props["P279"],
        subpropertyof=props["P1647"],
    )
