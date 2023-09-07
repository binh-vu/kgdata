from __future__ import annotations

from dataclasses import dataclass

import orjson
from rdflib.term import URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.page_id_dump import page_id_dump
from kgdata.misc.ntriples_parser import ntriple_loads
from kgdata.spark import are_records_unique


@dataclass
class DBpediaPageId:
    dbpedia_id: str
    wikipedia_id: int  # the numeric id of the wikipedia page article


def page_ids(lang: str = "en") -> Dataset[DBpediaPageId]:
    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        file_pattern=cfg.wikilinks / f"{lang}/*.gz", deserialize=deser_dbpedia_page_id
    )

    if not ds.has_complete_data():
        (
            page_id_dump(lang)
            .get_extended_rdd()
            .map(parse_pageid_triple)
            .map(lambda x: DBpediaPageId(x[0], x[1]))
            .map(ser_dbpedia_page_id)
            .save_like_dataset(ds)
        )

        # they are not unique, the following code is expected to fail
        rdd = ds.get_rdd()
        assert are_records_unique(rdd, lambda x: x.dbpedia_id)
        assert are_records_unique(rdd, lambda x: x.wikipedia_id)

    return ds


def deser_dbpedia_page_id(line):
    return DBpediaPageId(**orjson.loads(line))


def ser_dbpedia_page_id(obj: DBpediaPageId):
    return orjson.dumps(
        {"dbpedia_id": obj.dbpedia_id, "wikipedia_id": obj.wikipedia_id}
    )


PAGEID_PRED = URIRef("http://dbpedia.org/ontology/wikiPageID")
IntType = URIRef("http://www.w3.org/2001/XMLSchema#integer")


def parse_pageid_triple(line: str) -> tuple[str, int]:
    s, p, o = ntriple_loads(line)
    assert p == PAGEID_PRED, p
    assert o.datatype == IntType
    return str(s), o.value
