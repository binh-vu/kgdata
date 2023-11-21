from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Iterator

from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.cross_wiki_mapping import (
    WikipediaWikidataMapping,
    default_cross_wiki_mapping,
)
from kgdata.wikipedia.datasets.article_aliases import ArticleAliases, article_aliases
from kgdata.wikipedia.misc import get_title_from_url, is_wikipedia_url


@dataclass
class EntityAliases(Record):
    id: str
    aliases: dict[str, int]


def entity_wiki_aliases() -> Dataset[EntityAliases]:
    """This dataset isn't perfect, as there are some entities
    that have multiple articles, or vice versa"""

    cfg = WikidataDirCfg.get_instance()

    unfiltered_ds = Dataset(
        cfg.entity_wiki_aliases / "unfiltered/*.gz",
        deserialize=EntityAliases.deser,
        name="entity-wiki-aliases/unfiltered",
        dependencies=[
            default_cross_wiki_mapping(),
            article_aliases(),
        ],
    )
    ds = Dataset(
        cfg.entity_wiki_aliases / "filtered/*.gz",
        deserialize=EntityAliases.deser,
        name="entity-wiki-aliases/filtered",
        dependencies=[
            unfiltered_ds,
        ],
    )

    if not unfiltered_ds.has_complete_data():
        (
            default_cross_wiki_mapping()
            .get_extended_rdd()
            .map(lambda x: (x.wikipedia_title, x))
            .groupByKey()
            .join(
                article_aliases()
                .get_extended_rdd()
                .filter(lambda a: is_wikipedia_url(a.url))
                .map(lambda a: (get_title_from_url(a.url), a))
                .groupByKey()
            )
            .flatMap(merge_aliases)
            .map(EntityAliases.ser)
            .save_like_dataset(unfiltered_ds)
        )

    if not ds.has_complete_data():
        (
            unfiltered_ds.get_extended_rdd()
            .map(
                lambda e: EntityAliases(
                    e.id, {k: v for k, v in e.aliases.items() if is_useful(k.strip())}
                )
            )
            .map(EntityAliases.ser)
            .save_like_dataset(ds)
        )

    return ds


def merge_aliases(
    tup: tuple[
        str,
        tuple[Iterable[WikipediaWikidataMapping], Iterable[ArticleAliases]],
    ]
) -> list[EntityAliases]:
    wikipedia_title, (wiki_mappings, article_aliases) = tup

    out = []
    aliases = {}
    for alias in article_aliases:
        for k, c in alias.aliases.items():
            if k not in aliases:
                aliases[k] = c
            else:
                aliases[k] = max(aliases[k], c)

    if wikipedia_title not in aliases:
        aliases[wikipedia_title] = 0

    for wiki_mapping in wiki_mappings:
        out.append(
            EntityAliases(
                id=wiki_mapping.wikidata_entityid,
                aliases=aliases,
            )
        )

    return out


USELESS_ALIASES = {"↑", "●", "^", "*"}


def is_useful(alias: str) -> bool:
    return alias not in USELESS_ALIASES and all(
        re.match(pattern, alias) is None for pattern in [r"^\[?\d*\]?$", r"^\^\d+$"]
    )
