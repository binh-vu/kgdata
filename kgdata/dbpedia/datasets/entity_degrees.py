from __future__ import annotations

from dataclasses import dataclass
from operator import add
from typing import Optional
from urllib.parse import urlparse

from rdflib import URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.misc.resource import Record
from kgdata.models.entity import Entity
from kgdata.wikipedia.datasets.article_degrees import ArticleDegree, article_degrees
from kgdata.wikipedia.misc import get_title_from_url


@dataclass
class EntityDegree(Record):
    id: str
    indegree: int
    outdegree: int

    wikipedia_indegree: Optional[int] = None
    wikipedia_outdegree: Optional[int] = None


def entity_degrees(lang: str = "en") -> Dataset[EntityDegree]:
    cfg = DBpediaDirCfg.get_instance()

    step1_ds = Dataset(
        cfg.entity_degrees / "step1/*.gz",
        deserialize=EntityDegree.deser,
        name=f"entity-degrees/step1-{lang}",
        dependencies=[entities(lang)],
    )
    step2_ds = Dataset(
        cfg.entity_degrees / "step2/*.gz",
        deserialize=EntityDegree.deser,
        name=f"entity-degrees/{lang}",
        dependencies=[entities(lang), article_degrees(lang)],
    )

    if not step1_ds.has_complete_data():
        ent_rdd = entities(lang).get_extended_rdd()

        outdegree = ent_rdd.map(lambda e: (e.id, get_outdegree(e)))
        indegree = ent_rdd.flatMap(extract_indegree_links).reduceByKey(add)

        (
            outdegree.leftOuterJoin(indegree)
            .map(merge_degree)
            .map(EntityDegree.ser)
            .save_like_dataset(
                step1_ds,
                checksum=False,
                auto_coalesce=True,
            )
        )

    if not step2_ds.has_complete_data():
        (
            step1_ds.get_extended_rdd()
            .map(lambda e: (get_title_from_url(e.id, "/resource/"), e))
            .leftOuterJoin(
                article_degrees(lang)
                .get_extended_rdd()
                .map(lambda a: (get_title_from_url(a.url), a))
            )
            .map(merge_article_degree)
            .map(EntityDegree.ser)
            .save_like_dataset(
                step2_ds,
                auto_coalesce=True,
                trust_dataset_dependencies=True,
            )
        )

    return step2_ds


def wikipedia_to_dbpedia_url(url: str) -> str:
    parsedurl = urlparse(url)
    assert parsedurl.netloc.endswith("wikipedia.org")
    assert parsedurl.path.startswith("/wiki/")
    path = parsedurl.path.replace("/wiki/", "/resource/")
    return f"http://dbpedia.org{path}"


def merge_article_degree(
    tup: tuple[str, tuple[EntityDegree, Optional[ArticleDegree]]]
) -> EntityDegree:
    id, (ent, art) = tup
    if art is not None:
        ent.wikipedia_indegree = art.indegree
        ent.wikipedia_outdegree = art.outdegree
    return ent


def merge_degree(tup: tuple[str, tuple[int, Optional[int]]]) -> EntityDegree:
    url, (outdegree, indegree) = tup
    return EntityDegree(
        id=url, indegree=indegree if indegree is not None else 0, outdegree=outdegree
    )


def get_outdegree(e: Entity) -> int:
    return sum(len(vals) for vals in e.props.values())


def extract_indegree_links(e: Entity) -> list[tuple[str, int]]:
    out = []
    for vals in e.props.values():
        for val in vals:
            if isinstance(val, URIRef) and urlparse(str(val)).netloc == "dbpedia.org":
                out.append((str(val), 1))
    return out
