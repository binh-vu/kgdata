import orjson
import os
from operator import itemgetter
from typing import Tuple

from rdflib import Literal, URIRef

from kgdata.config import DBPEDIA_DIR
from kgdata.misc.ntriples_parser import ntriple_loads, ignore_comment
from kgdata.spark import get_spark_context, ensure_unique_records
from kgdata.wikipedia.rdd_datasets import id2groups

Triple = Tuple[str, str, str]


def rdfterm_to_json(term):
    """Convert the RDFTerm to JSON node. Does not support blank node"""
    if isinstance(term, URIRef):
        return str(term)
    if isinstance(term, Literal):
        value = term.value
        if isinstance(value, (int, float)):
            return value
        if term.datatype is None:
            if term.language is not None and term.language != "en":
                return {"value": value, "lang": term.language}
            return value
        return {"value": str(term), "datatype": str(term.datatype)}

    raise ValueError("Cannot convert a blank node: %s" % (str(term)))


def merge_triples(triples: list, auto_convert_rdfterm: bool = True):
    obj = {"@id": str(triples[0][0])}
    for s, p, o in triples:
        p = str(p)
        if auto_convert_rdfterm:
            o = rdfterm_to_json(o)

        if p in obj:
            if isinstance(obj[p], list):
                obj[p].append(o)
            else:
                obj[p] = [obj[p], o]
        else:
            obj[p] = o
    return obj


def wikipedia_links_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/wikipedia_links_en"),
    infile: str = "step_0/wikipedia-links_lang=en.ttl.bz2",
):
    sc = get_spark_context()

    step_1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step_1_outfile):
        # convert triples
        rdd = sc.textFile(os.path.join(indir, infile), use_unicode=False)
        rdd = rdd.map(lambda x: [str(term) for term in ntriple_loads(x)])

        # ignore irrelevant information
        rdd = rdd.filter(
            lambda triple: triple[1] == "http://xmlns.com/foaf/0.1/isPrimaryTopicOf"
        )
        rdd.map(lambda x: orjson.dumps(x).decode()).saveAsTextFile(
            step_1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
    else:
        rdd = sc.textFile(step_1_outfile, use_unicode=False).map(orjson.loads)

    # print("Numbers of records:", rdd.count())
    return rdd


def instance_types_specific_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/instance_types_specific_en"),
    infile: str = "step_0/instance-types_lang=en_specific.ttl.bz2",
):
    """Return the most specific type for each dbpedia resource.

    Each record in this dataset is guarantee to have unique subject, so that we can use it as an ID

    Parameters
    ----------
    indir : str, optional
        input directory, by default "${DATA_DIR}/dbpedia/cores/instance_types_specific_en"
    infile : str, optional
        input file, by default "step_0/instance-types_lang=en_specific.ttl.bz2"

    Returns
    -------
    RDD
        spark RDD of this dataset
    """
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(os.path.join(indir, infile), minPartitions=32)
        rdd = rdd.map(lambda x: [str(term) for term in ntriple_loads(x)])

        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
        ensure_unique_records(rdd, itemgetter(0))
    else:
        rdd = sc.textFile(step1_outfile).map(orjson.loads)

    return rdd


def fusion_instance_types_en(
    indir: str = os.path.join(DBPEDIA_DIR, "fusion/instance_types_en"),
    infile: str = "step_0/instance-types_reduce=dbpw_resolve=union.ttl.bz2",
):
    """This dataset is not very useful for me yet. DBPedia is a mess and not precise with what they release. So I think I just accept it and move on."""
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(os.path.join(indir, infile), minPartitions=32)
        rdd = rdd.map(lambda x: [str(term) for term in ntriple_loads(x)])
        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
    else:
        rdd = sc.textFile(step1_outfile).map(orjson.loads)

    print(
        rdd.filter(
            lambda x: x[0] == "http://dbpedia.org/resource/Stahlbahnwerke_Freudenstein"
        ).take(1)
    )

    return rdd


def mappingbased_literals_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/mappingbased_literals_en"),
    infile: str = "step_0/mappingbased-literals_lang=en.ttl.bz2",
):
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(
            os.path.join(indir, infile), minPartitions=32, use_unicode=False
        )
        rdd = (
            rdd.map(ntriple_loads)
            .map(lambda x: (str(x[0]), x))
            .groupByKey()
            .map(lambda x: list(x[1]))
            .map(merge_triples)
        )

        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
    else:
        rdd = sc.textFile(step1_outfile, use_unicode=False).map(orjson.loads)

    return rdd


def mappingbased_objects_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/mappingbased_objects_en"),
    infile: str = "step_0/mappingbased-objects_lang=en.ttl.bz2",
):
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(os.path.join(indir, infile), minPartitions=32)
        rdd = (
            rdd.filter(ignore_comment)
            .map(ntriple_loads)
            .map(lambda x: (str(x[0]), x))
            .groupByKey()
            .map(lambda x: list(x[1]))
            .map(merge_triples)
        )
        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
    else:
        rdd = sc.textFile(step1_outfile, use_unicode=False).map(orjson.loads)

    return rdd


def redirects_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/redirects_en"),
    infile: str = "step_0/redirects_lang=en.ttl.bz2",
):
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(
            os.path.join(indir, infile), minPartitions=32, use_unicode=False
        )
        rdd = rdd.map(lambda x: [str(term) for term in ntriple_loads(x)])

        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
    else:
        rdd = sc.textFile(step1_outfile, use_unicode=False).map(orjson.loads)

    return rdd


def pages_en(
    indir: str = os.path.join(DBPEDIA_DIR, "cores/pages_en"),
    infile: str = "step_0/page_lang=en_ids.ttl.bz2",
):
    """Load dataset of mapping between DBPedia resources and wiki article ID.

    Data of each record is going to be stored as: `{"@id": <id>, "http://dbpedia.org/ontology/wikiPageID": [<wikiID>, ...]}` because there is duplication of subjects in the data dumps
    due to redirections

    Parameters
    ----------
    indir : str, optional
        input directory, by default "${DATA_DIR}/dbpedia/cores/pages_en"
    infile : str, optional
        input fil, by default "step_0/page_lang=en_ids.ttl.bz2"

    Returns
    -------
    RDD
        spark RDD of this dataset
    """
    sc = get_spark_context()

    step1_outfile = os.path.join(indir, "step_1")
    if not os.path.exists(step1_outfile):
        # convert triples
        rdd = sc.textFile(os.path.join(indir, infile), minPartitions=32)
        rdd = (
            rdd.map(ntriple_loads)
            .map(lambda x: (str(x[0]), x))
            .groupByKey()
            .map(lambda x: list(x[1]))
            .map(merge_triples)
        )

        rdd.map(orjson.dumps).saveAsTextFile(
            step1_outfile,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )
        ensure_unique_records(rdd, itemgetter("@id"))
    else:
        rdd = sc.textFile(step1_outfile).map(orjson.loads)

    return rdd


def merge_instances_en(
    instance_types_rdd=None,
    mappingbased_literals_rdd=None,
    mappingbased_objects_rdd=None,
    pages_rdd=None,
    outfile: str = os.path.join(DBPEDIA_DIR, "instances_en/step_0"),
):
    def merge_triple_to_json(values):
        (s, p, o), b = values

        if b is None:
            return {"@id": s, p: o}
        if p in b:
            if isinstance(b[p], (list, tuple)):
                b[p].append(o)
            else:
                b[p] = [b[p], o]
        else:
            b[p] = o
        return b

    def merge_json_objects(values):
        a, b = values
        if b is None:
            return a

        assert a["@id"] == b["@id"], f"{a['@id']} != {b['@id']}"
        for k, v in b.items():
            if k not in a or k == "@id":
                a[k] = v
            else:
                if isinstance(a[k], list):
                    if isinstance(v, list):
                        a[k] += v
                    else:
                        a[k].append(v)
                else:
                    if isinstance(v, list):
                        v.append(a[k])
                        a[k] = v
                    else:
                        a[k] = [a[k], v]
        return a

    sc = get_spark_context()

    if not os.path.exists(outfile):
        rdd1 = (pages_rdd or pages_en()).map(lambda x: (x["@id"], x))
        rdd2 = (instance_types_rdd or instance_types_specific_en()).map(
            lambda x: (x[0], x)
        )

        rdd3 = (mappingbased_literals_rdd or mappingbased_literals_en()).map(
            lambda x: (x["@id"], x)
        )
        rdd4 = (mappingbased_objects_rdd or mappingbased_objects_en()).map(
            lambda x: (x["@id"], x)
        )

        rdd234 = (
            rdd2.leftOuterJoin(rdd3)
            .mapValues(merge_triple_to_json)
            .leftOuterJoin(rdd4)
            .mapValues(merge_json_objects)
        )

        rdd = (
            rdd1.leftOuterJoin(rdd234).mapValues(merge_json_objects).map(itemgetter(1))
        )
        rdd.map(orjson.dumps).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )
        ensure_unique_records(rdd, itemgetter("@id"))
    else:
        rdd = sc.textFile(outfile).map(orjson.loads)
    return rdd


def merged_instances_fixed_wiki_id_en(
    merge_instances_rdd=None,
    id2groups_rdd=None,
    outfile: str = os.path.join(DBPEDIA_DIR, "instances_en/step_1"),
):
    """This dataset is a subset of `merge_instances_en` that keeps only resources which wikiPageID is found in Wikipedia dump. It's resolved the redirect issue as well.

    The following properties are guarantee to be unique and can be use as id of an record:
        - '@id'
        - 'http://dbpedia.org/ontology/wikiPageID'

    Parameters
    ----------
    merge_instances_rdd : RDD, optional
        instances in DBPedia, by default None
    id2groups_rdd : RDD, optional
        a dataset that contains the mapping from wikipedia article id => resovled wikipedia articles id (obtained in `kg_data.wikipedia.rdd_dataset`)
    outfile : str, optional
        output file, by default "${DATA_DIR}/dbpedia/cores/instances_en/step_1"
    """
    sc = get_spark_context()
    if not os.path.exists(outfile):
        merge_instances_rdd = merge_instances_rdd or merge_instances_en()
        id2groups_rdd = id2groups_rdd or id2groups()

        def get_wiki_page_ids(x):
            if isinstance(x["http://dbpedia.org/ontology/wikiPageID"], list):
                return x["http://dbpedia.org/ontology/wikiPageID"]
            return [x["http://dbpedia.org/ontology/wikiPageID"]]

        def process_join_result(x):
            key, (instance, wiki_article) = x
            instance["http://dbpedia.org/ontology/wikiPageID"] = wiki_article["final"][
                1
            ]
            return instance

        merge_instances_rdd.flatMap(
            lambda x: [(wiki_id, x) for wiki_id in get_wiki_page_ids(x)]
        ).join(id2groups_rdd).map(process_join_result).map(orjson.dumps).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

        rdd = sc.textFile(outfile).map(orjson.loads)
        ensure_unique_records(rdd, itemgetter("@id"))
        ensure_unique_records(rdd, itemgetter("http://dbpedia.org/ontology/wikiPageID"))
    else:
        rdd = sc.textFile(outfile).map(orjson.loads)

    return rdd


if __name__ == "__main__":
    # instance_types_specific_en()
    # mappingbased_literals_en()
    # mappingbased_objects_en()
    # rdd = merge_instances_en()
    # rdd = merged_instances_fixed_wiki_id_en()
    # redirects_en()
    # wikipedia_links_en()
    # rdd = pages_en()
    # rdd = mappingbased_objects_en("${DATA_DIR}/dbpedia/cores/tmp", "mappingbased_objects_en.ttl.bz2")
    # rdd = mappingbased_objects_en()
    # res = rdd.filter(lambda x: x['@id'] == 'http://dbpedia.org/resource/Exhausting_Fire').collect()
    # print(res)
    rdd = fusion_instance_types_en()
