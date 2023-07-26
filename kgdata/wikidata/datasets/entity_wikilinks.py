from __future__ import annotations

from dataclasses import dataclass

import orjson
from kgdata.dataset import Dataset
from kgdata.dbpedia.datasets.wikilinks import WikiLink, wikilinks
from kgdata.spark import does_result_dir_exist, left_outer_join
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.wp2wd import wp2wd
from kgdata.wikidata.models.wdentitylink import WDEntityWikiLink


def entity_wikilinks(lang: str = "en") -> Dataset[WDEntityWikiLink]:
    """This dataset provides links between the entities that were found in Wikipedia. The dataset is generated
    by combining dbpedia.datasets.wikilinks and wikidata.datasets.wp2wd
    """
    cfg = WikidataDirCfg.get_instance()

    outdir = cfg.entity_wikilinks / lang
    if not does_result_dir_exist(outdir):
        link_ds = wikilinks(lang)
        wp2wd_ds = wp2wd(lang)

        newlink_rdd = left_outer_join(
            rdd1=link_ds.get_rdd(),
            rdd2=wp2wd_ds.get_rdd(),
            rdd1_keyfn=lambda x: x.source,
            rdd1_fk_fn=lambda x: [x.source] + x.targets,
            rdd2_keyfn=lambda x: x[0],
            join_fn=merge_link_and_wp2wd,
        )
        newlink_rdd.map(reformat_wikilink).filter(lambda x: x.source != "").map(
            ser_entity_wikilink
        ).saveAsTextFile(
            str(outdir), compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

        (outdir / "_METADATA").write_bytes(
            orjson.dumps({"wikilink_files": str(link_ds.file_pattern)})
        )

    return Dataset(str(outdir / "*.gz"), deserialize=deser_entity_wikilink)


def ser_entity_wikilink(obj: WDEntityWikiLink) -> bytes:
    return orjson.dumps(obj.to_dict())


def deser_entity_wikilink(obj: str | bytes) -> WDEntityWikiLink:
    return WDEntityWikiLink.from_dict(orjson.loads(obj))


def merge_link_and_wp2wd(
    r1: WikiLink, join_res: list[tuple[str, tuple[str, str] | None]]
):
    title2map = dict(join_res)
    r1source_map = title2map[r1.source]
    if r1source_map is not None:
        r1.source = r1source_map[1]
    else:
        r1.source = ""

    for i, target in enumerate(r1.targets):
        target_map = title2map[target]
        if target_map is not None:
            r1.targets[i] = target_map[1]
        else:
            r1.targets[i] = ""
    return r1


def reformat_wikilink(r1: WikiLink) -> WDEntityWikiLink:
    return WDEntityWikiLink(r1.source, {t for t in r1.targets if t != ""})
