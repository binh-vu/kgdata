import orjson
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.entities import entities
from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.misc.ntriples_parser import Triple, ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_file
from rdflib import URIRef


def redirection_dump(lang: str = "en"):
    cfg = DBpediaDirCfg.get_instance()

    if not does_result_dir_exist(cfg.redirection_dump / lang / "final"):
        split_a_file(
            infile=cfg.get_redirection_dump_file(lang),
            outfile=cfg.redirection_dump / lang / "raw" / f"part.ttl.gz",
            n_writers=8,
            override=False,
        )

        (
            get_spark_context()
            .textFile(str(cfg.redirection_dump / lang / "raw" / "*.gz"))
            .filter(ignore_comment)
            .map(ntriple_loads)
            .map(norm_redirection)  # extracted redirection (source -> target)
            .map(lambda x: (x[1], x))
            .groupByKey()  # group the redirection by targets (target -> [redirections])
            .join(
                entities().get_rdd().map(lambda r: (r.id, 1))
            )  # join with entities to filter out non-existing entities
            .flatMap(lambda x: x[1][0])  # get back the redirections
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.redirection_dump / lang / "final"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset.string(
        file_pattern=str(cfg.redirection_dump / lang / "final" / "*.gz")
    ).map(orjson.loads)


def norm_redirection(triple: Triple):
    assert triple[1] == URIRef("http://dbpedia.org/ontology/wikiPageRedirects")
    assert isinstance(triple[0], URIRef) and isinstance(triple[2], URIRef)
    return str(triple[0]), str(triple[2])
