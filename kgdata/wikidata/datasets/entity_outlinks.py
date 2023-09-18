from __future__ import annotations

from functools import partial

import orjson

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.models.entity import EntityOutLinks
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.cross_wiki_mapping import default_cross_wiki_mapping
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdvalue import WDValue
from kgdata.wikipedia.datasets.article_links import ArticleLinks, article_links
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url


def entity_outlinks() -> Dataset[EntityOutLinks]:
    """This dataset provides outgoing links from Wikidata entities, which is generated by combining wikidata and wikipedia datasets"""
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.entity_outlinks / "merged/*.gz",
        deserialize=partial(deser_from_dict, EntityOutLinks),
        name="entity-outlinks/merged",
        dependencies=[entities(), wiki_entity_outlinks()],
    )

    if not ds.has_complete_data():
        # convert article links to entity outlinks
        (
            entities()
            .get_extended_rdd()
            .map(extract_outlinks)
            .map(lambda e: (e.id, e))
            .join(wiki_entity_outlinks().get_extended_rdd().map(lambda e: (e.id, e)))
            .map(lambda g: EntityOutLinks(g[0], g[1][0].targets.union(g[1][1].targets)))
            .map(ser_to_dict)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def wiki_entity_outlinks() -> Dataset[EntityOutLinks]:
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        cfg.entity_outlinks / "wiki-article" / "*.gz",
        deserialize=partial(deser_from_dict, EntityOutLinks),
        name="entity-outlinks/wiki-article",
        dependencies=[default_cross_wiki_mapping(), article_links()],
    )

    if not ds.has_complete_data():
        title2id = (
            default_cross_wiki_mapping()
            .get_extended_rdd()
            .map(lambda x: (x.wikipedia_title, x.wikidata_entityid))
        )

        (
            article_links()
            .get_extended_rdd()
            .map(
                lambda a: (
                    a.name,
                    [
                        get_title_from_url(href.url)
                        for href in a.targets
                        if is_wikipedia_url(href.url)
                    ],
                )
            )
            .join(title2id)
            .map(lambda g: (g[1][1], g[1][0]))
            .flatMap(lambda x: [(t, x[0]) for t in x[1]])
            .join_repartition(title2id)
            .flatMap(lambda g: [(source, g[1][1]) for source in g[1][0]])
            .groupByKey()
            .map(lambda g: EntityOutLinks(g[0], set(g[1])))
            .map(ser_to_dict)
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def extract_outlinks(ent: WDEntity) -> EntityOutLinks:
    targets = set()
    for stmts in ent.props.values():
        for stmt in stmts:
            if WDValue.is_entity_id(stmt.value):
                targets.add(stmt.value.as_entity_id())
            for qvals in stmt.qualifiers.values():
                for qval in qvals:
                    if WDValue.is_entity_id(qval):
                        targets.add(qval.as_entity_id())

    return EntityOutLinks(ent.id, targets)