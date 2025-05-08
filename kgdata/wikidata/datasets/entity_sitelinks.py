from __future__ import annotations

from typing import Optional

import orjson
from kgdata.dataset import Dataset
from kgdata.db import ser_to_dict
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.datasets.triple_truthy_dump_derivatives import (
    triple_truthy_dump_derivatives,
)
from kgdata.wikidata.models.wdentity import EntitySiteLinks


def entity_sitelinks() -> Dataset[EntitySiteLinks]:
    cfg = WikidataDirCfg.get_instance()
    if cfg.has_json_dump():
        ds = Dataset(
            cfg.entity_sitelinks / "*.gz",
            deserialize=deser_sitelinks,
            name="entity-sitelinks",
            dependencies=[entities()],
        )

        if not ds.has_complete_data():
            (
                entities()
                .get_extended_rdd()
                .map(lambda ent: EntitySiteLinks(ent.id, ent.sitelinks))
                .map(ser_to_dict)
                .save_like_dataset(ds, auto_coalesce=True)
            )
    else:
        # ds = triple_truthy_dump_derivatives().sitelinks
        entity_sitelinks = list(cfg.dumps.glob("*entity_sitelinks*"))
        assert len(entity_sitelinks) == 1, entity_sitelinks

        prev_sitelinks_ds = Dataset(
            entity_sitelinks[0] / "*.gz",
            deserialize=deser_sitelinks,
            name="entity-sitelinks",
            dependencies=[],
        )

        ds = Dataset(
            cfg.entity_sitelinks / "*.gz",
            deserialize=deser_sitelinks,
            name="entity-sitelinks",
            dependencies=[prev_sitelinks_ds, entity_redirections()],
        )

        if not ds.has_complete_data():
            (
                prev_sitelinks_ds.get_extended_rdd()
                .map(lambda x: (x.id, x))
                .leftOuterJoin(entity_redirections().get_extended_rdd())
                .map(fix_redirect)
                .map(ser_to_dict)
                .save_like_dataset(ds, auto_coalesce=True)
            )

    return ds


def deser_sitelinks(line: str | bytes) -> EntitySiteLinks:
    return EntitySiteLinks.from_dict(orjson.loads(line))


def fix_redirect(tup: tuple[str, tuple[EntitySiteLinks, Optional[str]]]):
    id, (sitelinks, newid) = tup
    if newid is None:
        return sitelinks
    return EntitySiteLinks(newid, sitelinks.sitelinks)
