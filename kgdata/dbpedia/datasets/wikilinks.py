from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse

import orjson
from rdflib.term import URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.wikilink_dump import wikilink_dump
from kgdata.misc.ntriples_parser import ntriple_loads
from kgdata.spark import are_records_unique, does_result_dir_exist


@dataclass
class WikiLink:
    source: str
    targets: list[str]

    @staticmethod
    def from_dict(obj):
        return WikiLink(obj["source"], obj["targets"])

    def to_dict(self):
        return {"source": self.source, "targets": self.targets}


def wikilinks(lang: str = "en") -> Dataset[WikiLink]:
    cfg = DBpediaDirCfg.get_instance()

    outdir = cfg.wikilinks / lang

    if not does_result_dir_exist(outdir / "raw"):
        (
            wikilink_dump(lang)
            .get_rdd()
            .map(parse_wikilink)
            .groupByKey()
            .map(lambda x: {"source": x[0], "targets": list(x[1])})
            .map(orjson.dumps)
            .saveAsTextFile(
                str(outdir / "raw"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(outdir / "final"):
        ds = Dataset(file_pattern=outdir / "raw/*.gz", deserialize=orjson.loads)
        (
            ds.get_rdd()
            .map(convert2wikititle)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(outdir / "final"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

        rdd = Dataset(
            file_pattern=outdir / "final/*.gz", deserialize=deser_wikilink
        ).get_rdd()
        assert are_records_unique(rdd, lambda x: x.source)

    return Dataset(file_pattern=outdir / "final/*.gz", deserialize=deser_wikilink)


def merge_wikilinks(lst: list[WikiLink]):
    targets = {t for item in lst for t in item.targets}
    if len(targets) > max([len(item.targets) for item in lst]):
        raise Exception(
            "My understanding about the dump files from dbpedia may seem incorrect. Found duplicated objects with different targets.\n"
            + str(lst)
        )

    return WikiLink(lst[0].source, list(targets))


def deser_wikilink(line):
    return WikiLink.from_dict(orjson.loads(line))


IGNORE_URLS = {"http://dbpedia.org/", "http://dbpedia.org/resource/"}


def convert2wikititle(obj):
    return WikiLink(
        get_title_from_url(obj["source"]),
        [get_title_from_url(x) for x in obj["targets"] if x not in IGNORE_URLS],
    )


WIKILINK_PRED = URIRef("http://dbpedia.org/ontology/wikiPageWikiLink")


def parse_wikilink(line: str) -> tuple[str, str]:
    s, p, o = ntriple_loads(line)
    assert p == WIKILINK_PRED
    return str(s), str(o)


def get_title_from_url(url: str) -> str:
    """This function converts a dbpedia resource URL to wikipedia article title.

    Args:
        url: A dbpedia resource URL.

    Returns:
        A wikipedia page/article's title.
    """
    if ";" in url:
        # ; is old standard to split the parameter (similar to &) and is obsolete.
        # python 3.9, however, is still splitting it under this scheme http:// or https://
        # therefore, we need to address this special case by replacing the scheme.
        if url.startswith("http://") or url.startswith("https://"):
            url = "scheme" + url[4:]
        parsed_url = urlparse(url)
        path = parsed_url.path
        assert ";" in path
    else:
        parsed_url = urlparse(url)
        path = urlparse(url).path

    assert parsed_url.hostname == "dbpedia.org", url
    assert path.startswith("/resource/"), url
    path = path[10:]
    title = unquote(path).replace("_", " ")
    return title.strip()
