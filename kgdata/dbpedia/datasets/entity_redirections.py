from __future__ import annotations

import orjson
import serde.csv
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.misc.ntriples_parser import Triple, ignore_comment, ntriple_loads
from kgdata.spark.common import get_spark_context
from kgdata.spark.extended_rdd import ExtendedRDD
from kgdata.splitter import split_a_file
from rdflib import URIRef


def entity_redirections(lang: str = "en"):
    cfg = DBpediaDirCfg.get_instance()
    dump_date = cfg.get_dump_date()

    ds: Dataset[tuple[str, str]] = Dataset(
        cfg.entity_redirections / f"final-{lang}/*.gz",
        deserialize=orjson.loads,
        name=f"redirection-dump/{dump_date}-{lang}",
        dependencies=[entities(lang)],
    )

    if not ds.has_complete_data():
        split_a_file(
            infile=cfg.get_redirection_dump_file(lang),
            outfile=cfg.entity_redirections / f"raw-{lang}/part.ttl.gz",
            n_writers=8,
            override=False,
        )

        extra_redirections = serde.csv.deser(cfg.get_redirection_modified_file())

        (
            ExtendedRDD.textFile(cfg.entity_redirections / f"raw-{lang}/*.gz")
            .filter(ignore_comment)
            .map(ntriple_loads)
            .map(norm_redirection)  # extracted redirection (source -> target)
            .map(lambda x: (x[1], x))
            .groupByKey()  # group the redirection by targets (target -> [redirections])
            .join(
                entities(lang).get_extended_rdd().map(lambda r: (r.id, 1))
            )  # join with entities to filter out non-existing entities
            .flatMap(lambda x: x[1][0])  # get back the redirections
            .union(ExtendedRDD.parallelize(extra_redirections))
            .map(orjson.dumps)
            .save_like_dataset(
                ds, auto_coalesce=True, shuffle=True, trust_dataset_dependencies=True
            )
        )

    return ds


def norm_redirection(triple: Triple):
    assert triple[1] == URIRef("http://dbpedia.org/ontology/wikiPageRedirects")
    assert isinstance(triple[0], URIRef) and isinstance(triple[2], URIRef)
    return str(triple[0]), str(triple[2])
