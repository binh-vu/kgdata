from __future__ import annotations

from operator import add
from typing import Optional

from kgdata.dataset import Dataset
from kgdata.dbpedia.datasets.entity_degrees import EntityDegree
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.cross_wiki_mapping import cross_wiki_mapping
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikipedia.datasets.article_degrees import ArticleDegree, article_degrees
from kgdata.wikipedia.datasets.article_metadata import article_metadata
from kgdata.wikipedia.misc import get_title_from_url


def entity_degrees(lang: str = "en") -> Dataset[EntityDegree]:
    cfg = WikidataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.entity_degrees / "step1"):
        ent_rdd = entities(lang).get_rdd()

        outdegree = ent_rdd.map(lambda e: (e.id, get_outdegree(e)))
        indegree = ent_rdd.flatMap(extract_indegree_links).reduceByKey(add)

        (
            outdegree.leftOuterJoin(indegree)
            .map(merge_degree)
            .map(EntityDegree.ser)
            .coalesce(128)
            .saveAsTextFile(
                str(cfg.entity_degrees / "step1"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.entity_degrees / "step2"):
        (
            Dataset(cfg.entity_degrees / "step1/*.gz", deserialize=EntityDegree.deser)
            .get_rdd()
            .map(lambda e: (e.id, e))
            .leftOuterJoin(
                cross_wiki_mapping(article_metadata())
                .get_rdd()
                .map(lambda x: (x.wikipedia_title, x))
                .join(
                    article_degrees(lang)
                    .get_rdd()
                    .map(lambda a: (get_title_from_url(a.url), a))
                )
                .map(lambda tup: (tup[1][0].wikidata_entityid, tup[1][1]))
            )
            .map(merge_article_degree)
            .map(EntityDegree.ser)
            .coalesce(128)
            .saveAsTextFile(
                str(cfg.entity_degrees / "step2"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.entity_degrees / "step2/*.gz", deserialize=EntityDegree.deser)


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


def get_outdegree(e: WDEntity) -> int:
    return sum(len(stmts) for stmts in e.props.values())


def extract_indegree_links(e: WDEntity) -> list[tuple[str, int]]:
    out = []
    for stmts in e.props.values():
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                out.append((stmt.value.as_entity_id(), 1))
    return out
