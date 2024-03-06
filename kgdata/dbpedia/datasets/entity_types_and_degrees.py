from __future__ import annotations


from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entity_all_types import EntityAllTypes, entity_all_types
from kgdata.dbpedia.datasets.entity_degrees import EntityDegree, entity_degrees
from kgdata.wikidata.datasets.entity_types_and_degrees import EntityTypeAndDegree


def entity_types_and_degrees(lang: str = "en") -> Dataset[EntityTypeAndDegree]:
    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        cfg.entity_types_and_degrees / "*.gz",
        deserialize=EntityTypeAndDegree.deser,
        name=f"entity-types-and-degrees/{lang}",
        dependencies=[entity_all_types(lang), entity_degrees(lang)],
    )
    if not ds.has_complete_data():
        (
            entity_all_types(lang)
            .get_extended_rdd()
            .map(lambda e: (e.id, e))
            .join(entity_degrees(lang).get_extended_rdd().map(lambda e: (e.id, e)))
            .map(merge_type_degree)
            .map(EntityTypeAndDegree.ser)
            .save_like_dataset(
                ds,
                auto_coalesce=True,
                shuffle=True,
            )
        )

    return ds


def merge_type_degree(
    tup: tuple[str, tuple[EntityAllTypes, EntityDegree]]
) -> EntityTypeAndDegree:
    return EntityTypeAndDegree(
        id=tup[0],
        types=tup[1][0].types,
        indegree=tup[1][1].indegree,
        outdegree=tup[1][1].outdegree,
        wikipedia_indegree=tup[1][1].wikipedia_indegree,
        wikipedia_outdegree=tup[1][1].wikipedia_outdegree,
    )
