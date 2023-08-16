from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import orjson

from kgdata.dataset import Dataset
from kgdata.misc.resource import Record
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikipedia.datasets.article_metadata import ArticleMetadata
from kgdata.wikipedia.misc import get_title_from_url
from kgdata.wikipedia.models.html_article import HTMLArticle
from sm.misc.funcs import assert_not_null


@dataclass
class WikipediaWikidataMapping(Record):
    wikidata_entityid: str
    wikipedia_title: str
    # not all site are in <lang>wiki format such as zhwiktionary
    sites: list[str]


def cross_wiki_mapping(
    wiki_articles: Optional[Dataset[HTMLArticle] | Dataset[ArticleMetadata]] = None,
) -> Dataset[WikipediaWikidataMapping]:
    cfg = WikidataDirCfg.get_instance()

    need_verification = False

    if not does_result_dir_exist(cfg.cross_wiki_mapping / "step1"):
        (
            entities()
            .get_rdd()
            .flatMap(extract_sitelink)
            .map(WikipediaWikidataMapping.ser)
            .saveAsTextFile(
                str(cfg.cross_wiki_mapping / "step1"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

        need_verification = True

    ds = Dataset(
        cfg.cross_wiki_mapping / "step1/*.gz",
        deserialize=WikipediaWikidataMapping.deser,
    )
    if need_verification:
        rdd = ds.get_rdd().cache()

        # ensure that (title & entityid) is unique
        count = rdd.count()
        count1 = (
            rdd.map(lambda x: orjson.dumps([x.wikidata_entityid, x.wikipedia_title]))
            .distinct()
            .count()
        )
        assert count == count1, f"count: {count}, count1: {count1}"

    need_verification = False
    if wiki_articles is not None:
        sig = wiki_articles.get_signature()
        if not does_result_dir_exist(cfg.cross_wiki_mapping / "step2" / sig):
            (
                ds.get_rdd()
                .map(lambda x: (x.wikipedia_title, x))
                .groupByKey()
                .join(
                    wiki_articles.get_rdd().map(
                        lambda x: (get_title_from_url(x.url), x)
                    )
                )
                .map(resolve_multiple_mapping)
                .filter(lambda x: x is not None)
                .map(lambda e: WikipediaWikidataMapping.ser(assert_not_null(e)))
                .saveAsTextFile(
                    str(cfg.cross_wiki_mapping / "step2" / sig),
                    compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
                )
            )
            need_verification = True
        ds = Dataset(
            cfg.cross_wiki_mapping / f"step2/{sig}/*.gz",
            deserialize=WikipediaWikidataMapping.deser,
        )

    if need_verification:
        rdd = ds.get_rdd().cache()

        # ensure that (title & entityid) is unique after the list of articles are provided
        count = rdd.count()
        count1 = (
            rdd.map(lambda x: orjson.dumps([x.wikipedia_title, x.wikidata_entityid]))
            .distinct()
            .count()
        )
        assert count == count1, f"count: {count}, count1: {count1}"

        count2 = rdd.map(lambda x: x.wikipedia_title).distinct().count()
        assert count == count2, f"count: {count}, count1: {count2}"

    return ds


def resolve_multiple_mapping(
    tup: tuple[
        str, tuple[Iterable[WikipediaWikidataMapping], HTMLArticle | ArticleMetadata]
    ]
) -> Optional[WikipediaWikidataMapping]:
    title, (ents, article) = tup
    ents = list(ents)

    if article.wdentity is not None and any(
        ent.wikidata_entityid == article.wdentity for ent in ents
    ):
        # ents = [ent for ent in ents if ent.wikidata_entityid == article.wdentity]
        # if len(ents) == 1:
        #     return ents[0]

        # site = article.lang + "wiki"
        # ents = [ent for ent in ents if ent.site == site]
        # assert len(ents) == 1
        # return ents[0]

        assert len({ent.wikidata_entityid for ent in ents}) == len(
            ents
        ), "Should be unique due to previous check"
        ent = next(ent for ent in ents if ent.wikidata_entityid == article.wdentity)
        return ent

    # can not resolve in an easy way, match by site and language
    site = article.lang + "wiki"
    filtered_ents = [ent for ent in ents if site in ent.sites]
    if len(filtered_ents) == 0:
        return None
        # raise ValueError(
        #     f"Can not find mapping for {title} from sites: {[(ent.wikidata_entityid, ent.sites) for ent in ents]}."
        # )
    else:
        if len(filtered_ents) > 1:
            return None
            # raise ValueError(
            #     f"Ambiguous mapping for {title} from sites: {[(ent.wikidata_entityid, ent.sites) for ent in ents]}."
            # )
        ent = filtered_ents[0]
        return ent


def extract_sitelink(ent: WDEntity) -> list[WikipediaWikidataMapping]:
    title2sites = {}
    for sitelink in ent.sitelinks.values():
        title = sitelink.title
        if title not in title2sites:
            title2sites[title] = []
        title2sites[title].append(sitelink.site)

    out = []
    for title, sites in title2sites.items():
        out.append(WikipediaWikidataMapping(ent.id, title, sites))
    return out
