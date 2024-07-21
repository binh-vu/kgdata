from __future__ import annotations

import bz2
import xml.etree.ElementTree as ET
from functools import lru_cache
from io import BytesIO, StringIO
from pathlib import Path
from typing import Optional

import orjson
import requests
import serde.json
from kgdata.dataset import Dataset
from kgdata.spark.common import get_spark_context
from kgdata.spark.extended_rdd import ExtendedRDD
from kgdata.wikidata.config import WikidataDirCfg
from lxml import etree
from serde.helper import get_compression, get_open_fn
from tqdm.auto import tqdm


@lru_cache
def page_article_dump():
    cfg = WikidataDirCfg.get_instance()
    dump_date = cfg.get_dump_date()

    raw_xml_ds = Dataset(
        file_pattern=cfg.page_article_dump / "raw-xml/*.gz",
        deserialize=orjson.loads,
        name=f"page-article-dump/{dump_date}/raw",
        dependencies=[],
    )

    final_ds = Dataset(
        file_pattern=cfg.page_article_dump / "final/*.zst",
        deserialize=orjson.loads,
        name=f"page-article-dump/{dump_date}/final",
        dependencies=[raw_xml_ds],
    )

    if not raw_xml_ds.has_complete_data():
        files = cfg.get_page_article_dump_files()
        (
            ExtendedRDD.parallelize(files, numSlices=len(files))
            .flatMap(extract_page_article_dump)
            .map(lambda x: orjson.dumps(x.decode()))
            .save_like_dataset(raw_xml_ds, trust_dataset_dependencies=True)
        )

    if not final_ds.has_complete_data():
        fixed_dump_file = cfg.page_article_dump / "fixed-dumps.json"
        if Path(str(fixed_dump_file) + ".success").exists():
            assert fixed_dump_file.exists()
            id2entstr = serde.json.deser(fixed_dump_file)
        else:
            id2entstr = missing_entities(get_missing_entities(raw_xml_ds.get_rdd()))
            serde.json.ser(id2entstr, fixed_dump_file, indent=2)
            Path(str(fixed_dump_file) + ".success").touch()

        bc_id2entstr = get_spark_context().broadcast(id2entstr)
        (
            raw_xml_ds.get_extended_rdd()
            .map(lambda x: article_to_entity(x, bc_id2entstr))
            .filter(lambda x: x is not None)
            .save_like_dataset(final_ds)
        )

    return final_ds


3
RELEVANT_NAMESPACES = {"0", "120", "146"}


def article_to_entity(article: str, id2entstr) -> Optional[str]:
    el = etree.fromstring(article)

    ns = el.find("{*}ns").text
    if ns not in RELEVANT_NAMESPACES:
        return None

    id = el.find("{*}id").text
    title = el.find("{*}title").text

    (revision,) = el.findall("{*}revision")
    format = revision.find("{*}format").text

    assert format == "application/json"
    text = revision.find("{*}text").text
    if text is None:
        assert title in id2entstr.value
        return id2entstr.value[title]
    return text


def get_missing_entities(rdd):
    def missing_text(x):
        el = etree.fromstring(x)
        (revision,) = el.findall("{*}revision")
        format = revision.find("{*}format").text
        text = revision.find("{*}text").text

        if format == "application/json":
            return text is None
        return False

    return rdd.filter(missing_text).collect()


def missing_entities(articles: list):
    fixed = {}
    for article in tqdm(articles):
        el = etree.fromstring(article)
        title = el.find("{*}title").text

        (revision,) = el.findall("{*}revision")
        format = revision.find("{*}format").text

        assert format == "application/json"
        revision_id = revision.find("{*}id").text

        resp = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "prop": "revisions",
                "revids": revision_id,
                "rvprop": "timestamp|comment|content",
                "rvslots": "main",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        (data_page,) = data["query"]["pages"]
        assert data_page["title"] == title, (data_page["title"], title)
        (data_revision,) = data_page["revisions"]
        data_revision["slots"]["main"]["content"]

        fixed[title] = data_revision["slots"]["main"]["content"]
    return fixed[title]


def iter_page_article_dump(infile: Path):
    parser = etree.XMLPullParser(events=("start", "end"))
    with get_open_fn(infile)(infile, "rb") as f:
        for chunk in iter(lambda: f.read(16384), b""):
            parser.feed(chunk)
            for event, el in parser.read_events():
                yield event, el


def extract_page_article_dump(infile: Path):
    it = iter_page_article_dump(infile)

    # reparsing the header first to ensure the structure is right
    event, el = next(it)
    assert (
        event == "start"
        and el.tag == "{http://www.mediawiki.org/xml/export-0.10/}mediawiki"
    )

    event, el = next(it)
    assert (
        event == "start"
        and el.tag == "{http://www.mediawiki.org/xml/export-0.10/}siteinfo"
    )
    for event, el2 in it:
        if event == "end" and el2 is el:
            break
    assert el.find("{*}sitename").text == "Wikidata"
    el.clear()

    current_page = None
    tag_page = "{http://www.mediawiki.org/xml/export-0.10/}page"
    while True:
        event, el = next(it)
        if current_page is None:
            if event == "end":
                assert el.tag == "{http://www.mediawiki.org/xml/export-0.10/}mediawiki"
                break

            # expect a page, at the most upper level beside root
            assert el.tag == tag_page
            current_page = el
            continue

        if event == "end" and el.tag == tag_page:
            assert current_page is el
            # now we can process current_page
            yield etree.tostring(current_page)
            # clean_page = extract_page(current_page)
            # if clean_page is not None:
            #     yield clean_page
            current_page = None
            el.clear()

    # make sure we end with the root page
    try:
        next(it)
        assert False
    except StopIteration:
        pass
