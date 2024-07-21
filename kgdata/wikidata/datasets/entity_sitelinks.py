from __future__ import annotations

from dataclasses import dataclass

import orjson
from kgdata.dataset import Dataset
from kgdata.db import ser_to_dict
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import SiteLink


@dataclass
class EntitySiteLinks:
    id: str  # entity id
    sitelinks: dict[str, SiteLink]

    @staticmethod
    def from_dict(obj: dict) -> EntitySiteLinks:
        return EntitySiteLinks(
            obj["id"],
            {k: SiteLink.from_dict(v) for k, v in obj["sitelinks"].items()},
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sitelinks": {k: v.to_dict() for k, v in self.sitelinks.items()},
        }


def entity_sitelinks() -> Dataset[EntitySiteLinks]:
    cfg = WikidataDirCfg.get_instance()

    if cfg.has_json_dump():
        ds_deps = [entities()]
        ds_verify_dep_sigs = True
    else:
        ds_deps = []
        ds_verify_dep_sigs = False

    ds = Dataset(
        cfg.entity_sitelinks / "*.gz",
        deserialize=deser_sitelinks,
        name="entity-sitelinks",
        dependencies=ds_deps,
    )

    if not ds.has_complete_data(verify_dependencies_signature=ds_verify_dep_sigs):
        (
            entities()
            .get_extended_rdd()
            .map(lambda ent: EntitySiteLinks(ent.id, ent.sitelinks))
            .map(ser_to_dict)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def deser_sitelinks(line: str | bytes) -> EntitySiteLinks:
    return EntitySiteLinks.from_dict(orjson.loads(line))
