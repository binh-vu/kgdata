from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Iterable, Optional

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.cross_wiki_mapping import cross_wiki_mapping
from kgdata.wikipedia.datasets.mention_to_articles import mention_to_articles


@dataclass
class MentionToEntities:
    mention: str
    # entity id to two frequencies:
    # 0: global --- like concatenating all documents into a single document and count
    # 1: adjusted --- a mention may appear multiple times in a single document but we only count as 1
    target_entities: list[tuple[str, tuple[int, int]]]

    @staticmethod
    def from_dict(obj: dict) -> MentionToEntities:
        return MentionToEntities(obj["mention"], obj["target_entities"])

    def to_dict(self) -> dict:
        return {"mention": self.mention, "target_entities": self.target_entities}


def mention_to_entities() -> Dataset[MentionToEntities]:
    cfg = WikidataDirCfg.get_instance()

    ds = Dataset(
        file_pattern=cfg.mention_to_entities / "*.gz",
        deserialize=partial(deser_from_dict, MentionToEntities),
        name="mention-to-entities",
        dependencies=[cross_wiki_mapping(), mention_to_articles()],
    )

    if not ds.has_complete_data():
        (
            mention_to_articles()
            .get_extended_rdd()
            .flatMap(
                lambda x: [
                    (article, (x.mention, freqs))
                    for article, freqs in x.target_articles.items()
                ]
            )
            .groupByKey()
            .leftOuterJoin(
                cross_wiki_mapping()
                .get_extended_rdd()
                .map(lambda x: (x.wikipedia_title, x.wikidata_entityid))
                .groupByKey()
            )
            .flatMap(lambda x: merge_join(x[0], x[1][0], x[1][1]))
            .groupByKey()
            .map(
                lambda x: MentionToEntities(
                    x[0], sorted(x[1], key=lambda y: y[1][0], reverse=True)[:2048]
                )
            )
            .map(ser_to_dict)
            .save_like_dataset(ds)
        )

    return ds


def merge_join(
    title: str,
    mention_n_freqs: Iterable[tuple[str, tuple[int, int]]],
    entity_ids: Optional[Iterable[str]],
):
    output: list[tuple[str, tuple[str, tuple[int, int]]]] = []
    for mention, freqs in mention_n_freqs:
        if entity_ids is None:
            output.append((mention, ("", freqs)))
        else:
            for entity_id in entity_ids:
                output.append((mention, (entity_id, freqs)))
    return output
