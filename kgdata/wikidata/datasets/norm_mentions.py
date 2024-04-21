from __future__ import annotations

import re

import ftfy
import orjson
from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.mention_to_entities import mention_to_entities


def norm_mentions(
    with_deps: bool = True,
) -> Dataset[tuple[str, list[tuple[str, tuple[int, int]]]]]:
    cfg = WikidataDirCfg.get_instance()
    ds = Dataset(
        file_pattern=cfg.norm_mentions / "*.gz",
        deserialize=orjson.loads,
        name="norm-mentions",
        dependencies=[mention_to_entities()] if with_deps else [],
    )

    if not ds.has_complete_data():
        assert with_deps, "Dependencies are required to generate norm mentions"
        (
            mention_to_entities()
            .get_extended_rdd()
            .map(
                lambda x: (
                    normalize_mention(x.mention),
                    (
                        x.mention,
                        (
                            sum(freq[0] for _, freq in x.target_entities),
                            sum(freq[1] for _, freq in x.target_entities),
                        ),
                    ),
                )
            )
            .groupByKey()
            .map(lambda x: orjson.dumps([x[0], list(x[1])]))
            .save_like_dataset(ds, auto_coalesce=True)
        )

    return ds


def normalize_mention(mention: str):
    mention = " ".join(ftfy.fix_text(mention).replace("\xa0", " ").lower().split())
    mention = re.sub(r"[^\w\- ]+", "", mention).strip()
    return mention
