import copy, itertools, numpy as np, fastnumbers, math, shutil
from functools import reduce
import os, orjson, functools, warnings
import pickle, codecs
from operator import add, itemgetter, attrgetter
from collections import Counter, defaultdict
from loguru import logger
from kgdata.config import WIKIDATA_DIR
from kgdata.spark import (
    get_spark_context,
    ensure_unique_records,
    left_outer_join,
    does_result_dir_exist,
    cache_rdd,
)
from kgdata.wikipedia.prelude import get_title_from_url, title2groups
from kgdata.wikidata.deprecated.models.qnode import QNode
from kgdata.wikidata.models import WDProperty, WDClass


def qnodes(indir: str = os.path.join(WIKIDATA_DIR, "step_1")):
    # get qnodes from the dumps prepared by `s00_prep_data.py`. Return the data in the raw format as the qnode class
    # stores only data for a specific language!
    # see the data structure: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Site_Links
    sc = get_spark_context()
    return sc.textFile(os.path.join(indir, "*.gz")).map(orjson.loads)


def qnodes_en(
    qnodes_rdd=None,
    outfile: str = os.path.join(WIKIDATA_DIR, "step_2/qnodes_en"),
    raw: bool = False,
):
    """Get all QNodes in Wikidata (only english)"""
    sc = get_spark_context()
    if not does_result_dir_exist(outfile):
        qnodes_rdd = qnodes_rdd or qnodes()
        qnodes_rdd.map(functools.partial(QNode.from_wikidump, lang="en")).map(
            QNode.serialize
        ).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

        ensure_unique_records(
            sc.textFile(outfile).map(QNode.deserialize), keyfn=attrgetter("id")
        )

    if raw:
        return sc.textFile(outfile).map(orjson.loads)
    return sc.textFile(outfile).map(QNode.deserialize)


def qnodes_identifier(
    qnodes_rdd=None, outfile: str = os.path.join(WIKIDATA_DIR, "step_2/identifiers.txt")
):
    """Make a list of identifiers so that we can quickly identify if a node is missing or not"""
    if not os.path.exists(outfile):
        sc = get_spark_context()
        qnodes_rdd = qnodes_rdd or qnodes_en()
        ids_rdd = qnodes_rdd.map(lambda x: x.id).distinct()
        ids_rdd.coalesce(1).saveAsTextFile(outfile + ".tmp")
        assert os.path.join(outfile + ".tmp", "_SUCCESS")
        os.rename(os.path.join(outfile + ".tmp", "part-00000"), outfile)
        shutil.rmtree(outfile + ".tmp")


def wikidata_wikipedia_links(
    qnodes_rdd=None, outfile=os.path.join(WIKIDATA_DIR, "step_2/wiki_links")
):
    """Get alignments between wikidata qnode and wiki articles.

    Parameters
    ----------
    qnodes_rdd : RDD, optional
        RDD of all qnodes in wikidata, by default None
    outfile : str, optional
        output file, by default "/workspace/sm-dev/data/wikidata/step_2_links"

    Returns
    -------
    RDD
        RDD of { "id": <qnode>, "sitelinks": <sitelinks of qnode> }
    """
    sc = get_spark_context()

    if not does_result_dir_exist(outfile):
        # see the data structure: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Site_Links
        def p_0_discard_property(x):
            # only two types: property and item
            return x["type"] == "item"

        def p_1_keep_useful_props(x):
            return {"id": x["id"], "sitelinks": x["sitelinks"]}

        rdd = qnodes_rdd or qnodes()
        rdd.filter(p_0_discard_property).map(p_1_keep_useful_props).map(
            orjson.dumps
        ).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    return sc.textFile(outfile).map(orjson.loads)


def wiki_article_to_qnode(
    qnodes_rdd=None, outdir=os.path.join(WIKIDATA_DIR, "step_2/"), lang="en"
):
    site = f"{lang}wiki"
    outfile = os.path.join(outdir, f"{site}_links")

    if not does_result_dir_exist(outfile):

        def extract_links(qnode: QNode):
            if site not in qnode.sitelinks:
                return None
            title = qnode.sitelinks[site].title
            assert title is not None and isinstance(title, str) and len(title) > 0
            return title, qnode.id

        qnodes_rdd = qnodes_rdd or qnodes_en()
        # records = qnodes_rdd.filter(lambda x: 'enwiki' in x.sitelinks).take(5)
        # size = qnodes_rdd.map(extract_links).filter(lambda x: x is not None).count()
        # print(size)
        # return
        qnodes_rdd.map(extract_links).filter(lambda x: x is not None).map(
            orjson.dumps
        ).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )


def wiki_reltables_instances(
    qnodes_rdd=None,
    outfile: str = os.path.join(WIKIDATA_DIR, "step_2/wiki_reltables_instances"),
):
    """Get a subset of qnodes that have links with the wikipedia relational tables"""
    warnings.warn("wiki_reltables_instances is deprecated", DeprecationWarning)

    step0_file = os.path.join(outfile, "step_0")
    step1_file = os.path.join(outfile, "step_1")
    step2_file = os.path.join(outfile, "step_2")

    sc = get_spark_context()

    if not does_result_dir_exist(step0_file):
        # get qnode ids that connects with links in rel tables
        from kgdata.dbpedia.table_extraction import (
            wikipedia_links_in_relational_tables_en,
        )

        links_rdd = wikipedia_links_in_relational_tables_en()

        titles = set(links_rdd.map(itemgetter("title")).collect())
        titles = sc.broadcast(titles)

        def p_2_filter_no_wiki_link(item):
            # see the data structure: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Site_Links
            for x in item["sitelinks"].values():
                if x["title"] in titles.value:
                    return True
            return False

        rdd = wikidata_wikipedia_links()
        rdd.filter(p_2_filter_no_wiki_link).map(itemgetter("id")).coalesce(
            64
        ).saveAsTextFile(
            step0_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    if not does_result_dir_exist(step1_file):
        # create a list of target qnodes that qnodes in rel tables links to
        qnode_ids = sc.broadcast(set(sc.textFile(step0_file).collect()))

        def p_3_filter_by_qid(item):
            return item["id"] in qnode_ids.value

        def p_4_extract_link_qid(qnode):
            qnode = QNode.from_wikidump(qnode, lang="en")
            targets = [qnode.id]
            for stmts in qnode.props.values():
                for stmt in stmts:
                    if stmt.value.is_qnode():
                        targets.append(stmt.value.as_entity_id())

            return targets

        qnodes_rdd = qnodes_rdd or qnodes()
        qnodes_rdd.filter(p_3_filter_by_qid).flatMap(
            p_4_extract_link_qid
        ).distinct().saveAsTextFile(
            step1_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    step20_file = os.path.join(step2_file, "step_0")
    step21_file = os.path.join(step2_file, "step_1")
    step22_file = os.path.join(step2_file, "step_2")

    if not os.path.exists(os.path.join(step2_file, "_SUCCESS")):
        # create a final list that join those qnodes together
        if not does_result_dir_exist(step20_file):
            qnode_ids = sc.broadcast(set(sc.textFile(step1_file).collect()))

            def p_5_filter_by_qid(item):
                return item["id"] in qnode_ids.value

            qnodes_rdd = qnodes_rdd or qnodes()
            qnodes_rdd.filter(p_5_filter_by_qid).map(
                functools.partial(QNode.from_wikidump, lang="en")
            ).map(QNode.serialize).saveAsTextFile(
                step20_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )

        if not does_result_dir_exist(step21_file):
            # create rdd1 of records in rel tables
            original_qnode_ids = sc.broadcast(set(sc.textFile(step0_file).collect()))

            def p_6_filter_by_qid(item):
                return item.id in original_qnode_ids.value

            sc.textFile(step20_file).map(QNode.deserialize).filter(
                p_6_filter_by_qid
            ).map(QNode.serialize).saveAsTextFile(
                step21_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )

        if not does_result_dir_exist(step22_file):
            # create rdd2 of records that we want to join
            def p_7_extract_essential_info(qnode):
                return {"id": qnode.id, "label": qnode.label}

            def rdd1_fk_fn(qnode: QNode):
                targets = set()
                for stmts in qnode.props.values():
                    for stmt in stmts:
                        if stmt.value.is_qnode():
                            targets.add(stmt.value.as_entity_id())
                return list(targets)

            def rdd1_join_fn(qnode: QNode, targets):
                targets = dict(targets)
                for stmts in qnode.props.values():
                    for stmt in stmts:
                        if stmt.value.is_qnode():
                            qnode_id = stmt.value.as_entity_id()
                            if targets[qnode_id] is not None:
                                stmt.value.set_qnode_label(targets[qnode_id]["label"])
                            else:
                                stmt.value.set_qnode_label(None)
                return targets

            rdd1 = sc.textFile(step21_file).map(QNode.deserialize)
            rdd2 = (
                sc.textFile(step20_file)
                .map(QNode.deserialize)
                .map(p_7_extract_essential_info)
            )
            rdd1_keyfn = attrgetter("id")
            rdd2_keyfn = itemgetter("id")

            left_outer_join(
                rdd1,
                rdd2,
                rdd1_keyfn,
                rdd1_fk_fn,
                rdd2_keyfn,
                rdd1_join_fn,
                QNode.serialize,
                step22_file,
                compression=True,
            )

        with open(os.path.join(step2_file, "_SUCCESS"), "w") as f:
            f.write("DONE")

    return sc.textFile(step22_file).map(QNode.deserialize)


def wikidata_graph_structure(
    qnodes_rdd=None, outfile: str = os.path.join(WIKIDATA_DIR, "step_2/graph_structure")
):
    """Get the RDD data that contains incoming links between nodes and outgoing links between nodes.
    The graph ignores the instance of class to prevent one node has so many links
    """
    qnodes_rdd = qnodes_rdd or qnodes_en()
    step0_file = os.path.join(outfile, "outedges")
    step1_file = os.path.join(outfile, "inedges")
    step2_file = os.path.join(outfile, "data")

    if not does_result_dir_exist(step0_file):
        logger.info("Extract outgoing links...")

        def extract_outedges(qnode: QNode):
            outedges = {}
            for p, stmts in qnode.props.items():
                if p == "P31":
                    continue

                for stmt in stmts:
                    if stmt.value.is_qnode():
                        if p not in outedges:
                            outedges[p] = set()
                        outedges[p].add(stmt.value.as_entity_id())

                    for q, qvals in stmt.qualifiers.items():
                        edge = f"{p}-{q}"
                        for qval in qvals:
                            if qval.is_qnode():
                                if edge not in outedges:
                                    outedges[edge] = set()
                                outedges[edge].add(qval.as_entity_id())

            for edge in outedges:
                outedges[edge] = list(outedges[edge])

            return {"id": qnode.id, "outedges": outedges}

        qnodes_rdd.map(extract_outedges).map(orjson.dumps).saveAsTextFile(
            step0_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    if not does_result_dir_exist(step1_file):
        logger.info("Extract incoming links...")
        sc = get_spark_context()
        rdd = sc.textFile(step0_file).map(orjson.loads)

        def inverse(odict):
            inverse_qnodes = {}
            for p, qnodes in odict["outedges"].items():
                for qnode in qnodes:
                    if qnode not in inverse_qnodes:
                        inverse_qnodes[qnode] = {"id": qnode, "inedges": {}}
                    if p not in inverse_qnodes[qnode]["inedges"]:
                        inverse_qnodes[qnode]["inedges"][p] = set()
                    inverse_qnodes[qnode]["inedges"][p].add(odict["id"])
            return [(n["id"], n) for n in inverse_qnodes.values()]

        def combine(a, b):
            for p, pvals in b["inedges"].items():
                if p in a["inedges"]:
                    a["inedges"][p].update(pvals)
                else:
                    a["inedges"][p] = pvals
            return a

        rdd.flatMap(inverse).reduceByKey(combine).map(itemgetter(1)).map(
            lambda x: orjson.dumps(
                x, option=orjson.OPT_SERIALIZE_DATACLASS, default=list
            )
        ).saveAsTextFile(
            step1_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    if not does_result_dir_exist(step2_file):
        sc = get_spark_context()
        outedges = sc.textFile(step0_file).map(orjson.loads).map(lambda x: (x["id"], x))
        inedges = sc.textFile(step1_file).map(orjson.loads).map(lambda x: (x["id"], x))

        def merge_nodes(args):
            key, (inedge, outedge) = args
            return {
                "id": key,
                "inedges": inedge["inedges"] if inedge is not None else {},
                "outedges": outedge["outedges"] if outedge is not None else {},
            }

        inedges.fullOuterJoin(outedges).map(merge_nodes).map(
            orjson.dumps
        ).saveAsTextFile(
            step2_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    return get_spark_context().textFile(step2_file).map(orjson.loads)


def wikidata_schema(
    qnodes_rdd=None, outfile: str = os.path.join(WIKIDATA_DIR, "step_2/schema")
):
    """Create a schema of Wikidata.

    1. `node_classes` contains a list of qnodes, each with its classes, its outgoing qnodes (not props) and their classes.
    2. `node_degree` contains a list of qnodes and their degree
    3. `node_schema` contains schema of each qnode
    4. `class_schema` schema of class computed from `node_schema`
    """
    qnodes_rdd = qnodes_rdd or qnodes_en()

    node_class_file = os.path.join(outfile, "node_classes")
    node_degree_file = os.path.join(outfile, "node_degree")
    node_schema_file = os.path.join(outfile, "node_schema")
    class_schema_file = os.path.join(outfile, "class_schema")

    def extract_classes(qnode: QNode):
        out_qnodes = set()
        for p, stmts in qnode.props.items():
            for stmt in stmts:
                if stmt.value.is_qnode():
                    out_qnodes.add(stmt.value.as_entity_id())

                for q, qvals in stmt.qualifiers.items():
                    for qval in qvals:
                        if qval.is_qnode():
                            out_qnodes.add(qval.as_entity_id())

        return {
            "id": qnode.id,
            "classes": list(
                {stmt.value.as_entity_id() for stmt in qnode.props.get("P31", [])}
            ),
            "out_qnodes": list(out_qnodes),
        }

    if not does_result_dir_exist(node_class_file):

        def join(x, ys):
            ys = dict(ys)
            return {
                "id": x["id"],
                "classes": list(x["classes"]),
                "out_qnodes": {
                    k2: list(ys[k2]["classes"])
                    for k2 in x["out_qnodes"]
                    if ys[k2] is not None
                },
            }

        node_class_rdd = qnodes_rdd.map(extract_classes).persist()
        left_outer_join(
            rdd1=node_class_rdd,
            rdd2=node_class_rdd.map(lambda x: {"id": x["id"], "classes": x["classes"]}),
            rdd1_keyfn=itemgetter("id"),
            rdd1_fk_fn=itemgetter("out_qnodes"),
            rdd2_keyfn=itemgetter("id"),
            join_fn=join,
            ser_fn=orjson.dumps,
            outfile=node_class_file,
        )

    if not does_result_dir_exist(node_degree_file):

        def extract_freq(x):
            count = [(x["id"], len(x["out_qnodes"]))]
            for xi in x["out_qnodes"]:
                count.append((xi, 1))
            return count

        qnodes_rdd.map(extract_classes).flatMap(extract_freq).foldByKey(0, add).map(
            orjson.dumps
        ).saveAsTextFile(
            node_degree_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    if not does_result_dir_exist(node_schema_file):

        def extract_qnode_schema(qnode: QNode):
            schema = {}
            for prop, stmts in qnode.props.items():
                if prop == "P31":
                    continue
                # storing the values of objects & literals for calculating the probability of class & datatype
                prop_qnodes = defaultdict(int)
                prop_literal_dtypes = defaultdict(int)

                qualifier_qnodes = defaultdict(lambda: defaultdict(int))
                qualifier_dtypes = defaultdict(lambda: defaultdict(int))
                n_qualifiers = defaultdict(int)
                for stmt in stmts:
                    if stmt.value.is_qnode():
                        prop_qnodes[stmt.value.as_entity_id()] += 1
                    else:
                        prop_literal_dtypes[stmt.value.type] += 1

                    for qual, qual_values in stmt.qualifiers.items():
                        for qual_val in qual_values:
                            if qual_val.is_qnode():
                                qualifier_qnodes[qual][qual_val.as_entity_id()] += 1
                            else:
                                qualifier_dtypes[qual][qual_val.type] += 1
                        n_qualifiers[qual] += 1

                schema[prop] = {
                    "n_stmts": len(stmts),
                    "qnodes": dict(prop_qnodes),
                    "literal_types": dict(prop_literal_dtypes),
                    "qualifiers": {
                        qual: {
                            "qnodes": dict(qualifier_qnodes[qual]),
                            "literal_types": dict(qualifier_dtypes[qual]),
                            "n_qualifiers": n_qualifiers[qual],
                        }
                        for qual in qualifier_qnodes
                    },
                }

            return qnode.id, schema

        sc = get_spark_context()

        # building an rdd that contain class information of outgoing qnodes and a weight that try to represent the centrality of a node
        node_class_rdd = (
            sc.textFile(node_class_file).map(orjson.loads).map(lambda x: (x["id"], x))
        )
        node_degree_rdd = sc.textFile(node_degree_file).map(orjson.loads)

        def merge(args):
            k, (gclass, gcen) = args
            gclass["degree"] = gcen
            return gclass["id"], gclass

        vertice_rdd = node_class_rdd.leftOuterJoin(node_degree_rdd).map(merge)

        # merge the qnode schema with the rdd above to complete the schema with qnode classes
        def merge_class_info(args):
            qnode_id, (qnode_schema, qnode_linked_info) = args
            for prop, prop_schema in qnode_schema.items():
                class_freqs = defaultdict(int)
                for qid, qnode_count in prop_schema.pop("qnodes").items():
                    for clsid in qnode_linked_info["out_qnodes"].get(qid, []):
                        class_freqs[clsid] += qnode_count
                prop_schema["classes"] = dict(class_freqs)

                for qual, qual_schema in prop_schema["qualifiers"].items():
                    class_freqs = defaultdict(int)
                    for qid, qnode_count in qual_schema.pop("qnodes").items():
                        for clsid in qnode_linked_info["out_qnodes"].get(qid, []):
                            class_freqs[clsid] += qnode_count
                    qual_schema["classes"] = dict(class_freqs)

            return {
                "id": qnode_id,
                "classes": qnode_linked_info["classes"],
                "schema": qnode_schema,
                "degree": qnode_linked_info["degree"],
            }

        qnodes_rdd.map(extract_qnode_schema).leftOuterJoin(vertice_rdd).map(
            merge_class_info
        ).map(orjson.dumps).coalesce(2048).saveAsTextFile(
            node_schema_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    if not does_result_dir_exist(class_schema_file):

        def cls2instance(x):
            instances = []
            for clsid in x["classes"]:
                cschema = {"id": clsid, "n_instances": 1, "props": {}}
                for prop, prop_schema in x["schema"].items():
                    prop_schema = copy.deepcopy(prop_schema)
                    prop_schema["n_instances"] = 1
                    example = {"objects": {}, "literals": {}, "qualifiers": {}}
                    for c in prop_schema["classes"]:
                        example["objects"][c] = [(x["id"], x["degree"])]
                    for c in prop_schema["literal_types"]:
                        example["literals"][c] = [(x["id"], x["degree"])]
                    for qual, qual_schema in prop_schema["qualifiers"].items():
                        example["qualifiers"][qual] = {"objects": {}, "literals": {}}
                        for c in qual_schema["classes"]:
                            example["qualifiers"][qual]["objects"][c] = [
                                (x["id"], x["degree"])
                            ]
                        for c in qual_schema["literal_types"]:
                            example["qualifiers"][qual]["literals"][c] = [
                                (x["id"], x["degree"])
                            ]
                    prop_schema["examples"] = example
                    assert len(example["objects"]) == len(prop_schema["classes"])
                    cschema["props"][prop] = prop_schema

                instances.append((clsid, cschema))
            return instances

        def merge_schema(schema1, schema2):
            if isinstance(schema1, tuple):
                return schema1
            if isinstance(schema2, tuple):
                return schema2

            assert schema1["id"] == schema2["id"]
            top_k_examples = 5

            schema1["n_instances"] += schema2["n_instances"]
            for prop, prop_schema2 in schema2["props"].items():
                if prop not in schema1["props"]:
                    schema1["props"][prop] = prop_schema2
                    continue

                # merge exist prop schema
                prop_schema1 = schema1["props"][prop]
                prop_schema1["n_instances"] += prop_schema2["n_instances"]
                prop_schema1["n_stmts"] += prop_schema2["n_stmts"]
                for k, v in prop_schema2["classes"].items():
                    if k not in prop_schema1["classes"]:
                        prop_schema1["classes"][k] = v
                        prop_schema1["examples"]["objects"][k] = prop_schema2[
                            "examples"
                        ]["objects"][k]
                    else:
                        prop_schema1["classes"][k] += v
                        prop_schema1["examples"]["objects"][k] = sorted(
                            prop_schema1["examples"]["objects"][k]
                            + prop_schema2["examples"]["objects"][k],
                            key=itemgetter(1),
                            reverse=True,
                        )[:top_k_examples]
                for k, v in prop_schema2["literal_types"].items():
                    if k not in prop_schema1["literal_types"]:
                        prop_schema1["literal_types"][k] = v
                        prop_schema1["examples"]["literals"][k] = prop_schema2[
                            "examples"
                        ]["literals"][k]
                    else:
                        prop_schema1["literal_types"][k] += v
                        prop_schema1["examples"]["literals"][k] = sorted(
                            prop_schema1["examples"]["literals"][k]
                            + prop_schema2["examples"]["literals"][k],
                            key=itemgetter(1),
                            reverse=True,
                        )[:top_k_examples]

                for qual, qual_schema2 in prop_schema2["qualifiers"].items():
                    if qual not in prop_schema1["qualifiers"]:
                        prop_schema1["qualifiers"][qual] = prop_schema2["qualifiers"][
                            qual
                        ]
                        prop_schema1["examples"]["qualifiers"][qual] = prop_schema2[
                            "examples"
                        ]["qualifiers"][qual]
                    else:
                        qual_schema1 = prop_schema1["qualifiers"][qual]
                        qual_schema1["n_qualifiers"] += qual_schema2["n_qualifiers"]
                        for k, v in qual_schema2["classes"].items():
                            if k not in qual_schema1["classes"]:
                                qual_schema1["classes"][k] = v
                                prop_schema1["examples"]["qualifiers"][qual]["objects"][
                                    k
                                ] = prop_schema2["examples"]["qualifiers"][qual][
                                    "objects"
                                ][
                                    k
                                ]
                            else:
                                qual_schema1["classes"][k] += v
                                prop_schema1["examples"]["qualifiers"][qual]["objects"][
                                    k
                                ] = sorted(
                                    prop_schema1["examples"]["qualifiers"][qual][
                                        "objects"
                                    ][k]
                                    + prop_schema2["examples"]["qualifiers"][qual][
                                        "objects"
                                    ][k],
                                    key=itemgetter(1),
                                    reverse=True,
                                )[
                                    :top_k_examples
                                ]
                        for k, v in qual_schema2["literal_types"].items():
                            if k not in qual_schema1["literal_types"]:
                                qual_schema1["literal_types"][k] = v
                                prop_schema1["examples"]["qualifiers"][qual][
                                    "literals"
                                ][k] = prop_schema2["examples"]["qualifiers"][qual][
                                    "literals"
                                ][
                                    k
                                ]
                            else:
                                qual_schema1["literal_types"][k] += v
                                prop_schema1["examples"]["qualifiers"][qual][
                                    "literals"
                                ][k] = sorted(
                                    prop_schema1["examples"]["qualifiers"][qual][
                                        "literals"
                                    ][k]
                                    + prop_schema2["examples"]["qualifiers"][qual][
                                        "literals"
                                    ][k],
                                    key=itemgetter(1),
                                    reverse=True,
                                )[
                                    :top_k_examples
                                ]

            return schema1

        # get_spark_context().textFile(node_schema_file) \
        #     .map(orjson.loads).flatMap(cls2instance) \
        #     .filter(lambda x: x[0] == 'Q2367225') \
        #     .map(orjson.dumps) \
        #     .saveAsTextFile(node_schema_file + "_tmp", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
        # lst = get_spark_context().textFile(node_schema_file).map(orjson.loads).filter(lambda x: x['id'] == 'Q30').count()
        # print(lst)
        # return

        # def test_irregular(schema):
        #     for p in schema['props'].values():
        #         for lst in p['examples']['objects'].values():
        #             if lst[0][0] == 'Q30':
        #                 return True
        #     return False
        # result = get_spark_context().textFile(node_schema_file + "_tmp").map(orjson.loads).map(itemgetter(1)).filter(test_irregular).collect()

        # print(len(result))
        # x = get_spark_context().textFile(class_schema_file).map(lambda x: x.split("\t", 1)[1]).map(orjson.loads).filter(lambda x: x['id'] == 'Q2367225').collect()
        # print(x[0]['props']['P118'])
        # result2 = get_spark_context().textFile(class_schema_file).map(lambda x: x.split("\t", 1)[1]).map(orjson.loads).filter(lambda x: x['id'] == 'Q2367225').filter(test_irregular).collect()
        # print(len(result2))

        # print("here")
        # return
        #     .reduceByKey(merge_schema) \
        #     .count()
        # print(result)
        # # prop_schema1, prop_schema2, schema1, schema2 = result[0][1]
        # return
        # import IPython
        # IPython.embed()

        rdd = (
            get_spark_context()
            .textFile(node_schema_file)
            .map(orjson.loads)
            .flatMap(cls2instance)
            .reduceByKey(merge_schema)
            .map(lambda x: x[0] + "\t" + orjson.dumps(x[1]).decode())
            .coalesce(512)
            .saveAsTextFile(
                class_schema_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    def extract(line: str):
        id, data = line.split("\t")
        return id, orjson.loads(data)

    return get_spark_context().textFile(class_schema_file).map(extract)


def wikidata_quantity_prop_stats(
    qnodes_rdd=None,
    outfile: str = os.path.join(WIKIDATA_DIR, "step_2/quantity_prop_stats"),
):
    qnodes_rdd = qnodes_rdd or qnodes_en()

    raw_quantity_values_file = os.path.join(outfile, "raw_quantity_values")
    quantity_stats_file = os.path.join(outfile, "quantity_stats")

    if not does_result_dir_exist(raw_quantity_values_file):
        wd_props = WDProperty.from_file()
        identifiers = {p.id for p in wd_props.values() if p.datatype == "external-id"}
        identifiers = get_spark_context().broadcast(identifiers)

        def extract_quantity(val: dict):
            assert len(val) <= 4, f"So that they only have 4 props: {val.keys()}"
            return (
                val["amount"],
                val.get("unit", None),
                val.get("upperBound", None),
                val.get("lowerBound", None),
            )

        def extract_data_prop(qnode: QNode):
            raw_dprop_values = []
            for p, stmts in qnode.props.items():
                # ignore identifiers
                if p in identifiers.value:
                    continue
                p_vals = []
                p_qual_values = {}
                for stmt in stmts:
                    if stmt.value.is_quantity():
                        p_vals.append(extract_quantity(stmt.value.value))

                    for q, qvals in stmt.qualifiers.items():
                        new_qvals = []
                        for qval in qvals:
                            if qval.is_quantity():
                                new_qvals.append(extract_quantity(qval.value))
                        if len(new_qvals) > 0:
                            p_qual_values[q] = new_qvals
                if len(p_vals) > 0 or len(p_qual_values) > 0:
                    raw_dprop_values.append(
                        (p, {"id": p, "value": p_vals, "qualifiers": p_qual_values})
                    )
            return raw_dprop_values

        qnodes_rdd.flatMap(extract_data_prop).map(orjson.dumps).saveAsTextFile(
            raw_quantity_values_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    if not does_result_dir_exist(quantity_stats_file):
        # max_f32 = np.finfo(np.float32).max
        max_i36 = np.iinfo(np.int32).max * 16

        def create_stats(pinfo):
            defaultstats = {
                "units": set(),
                "size": 0,
                "int_size": 0,
                "min": float("inf"),
                "max": -float("inf"),
                "sum": 0,
                "sum_square": 0,
                "n_overi36": 0,
            }
            value = add_stats(copy.copy(defaultstats), pinfo["value"])
            qualifiers = {
                q: add_stats(copy.copy(defaultstats), qvals)
                for q, qvals in pinfo["qualifiers"].items()
            }
            return {"id": pinfo["id"], "value": value, "qualifiers": qualifiers}

        def add_stats(statdict, quan_lst):
            if len(quan_lst) == 0:
                return statdict
            units = list({x[1] for x in quan_lst})
            olst = [fastnumbers.fast_real(x[0]) for x in quan_lst]
            assert all(isinstance(x, (int, float)) and not math.isinf(x) for x in olst)

            lst = [x for x in olst if x <= max_i36]

            statdict = copy.copy(statdict)
            statdict["units"] = statdict["units"].union(units)
            statdict["size"] += len(olst)
            statdict["int_size"] += sum([isinstance(x, int) for x in olst])
            if len(lst) > 0:
                statdict["min"] = min(statdict["min"], min(lst))
                statdict["max"] = max(statdict["max"], max(lst))
            statdict["sum"] += sum(lst)
            statdict["sum_square"] += sum([x**2 for x in lst])
            statdict["n_overi36"] += len(olst) - len(lst)
            return statdict

        def merge_stats(odict1, odict2):
            statdict = copy.copy(odict1)
            statdict["units"] = statdict["units"].union(odict2["units"])
            statdict["size"] += odict2["size"]
            statdict["int_size"] += odict2["int_size"]
            statdict["min"] = min(statdict["min"], odict2["min"])
            statdict["max"] = max(statdict["max"], odict2["max"])
            statdict["sum"] += odict2["sum"]
            statdict["sum_square"] += odict2["sum_square"]
            statdict["n_overi36"] += odict2["n_overi36"]
            return statdict

        def compute_statistic(p1info, p2info):
            assert p1info["id"] == p2info["id"]
            pinfo = {"id": p1info["id"]}
            pinfo["value"] = merge_stats(p1info["value"], p2info["value"])
            pinfo["qualifiers"] = copy.copy(p1info["qualifiers"])
            for q, qinfo in p2info["qualifiers"].items():
                if q not in pinfo["qualifiers"]:
                    pinfo["qualifiers"][q] = copy.copy(qinfo)
                else:
                    pinfo["qualifiers"][q] = merge_stats(pinfo["qualifiers"][q], qinfo)
            return pinfo

        def compute_statistic_postprocess(pinfo):
            lst = [pinfo["value"]] + list(pinfo["qualifiers"].values())
            for item in lst:
                if item["size"] == 0:
                    item["units"] = set()
                    item["min"] = None
                    item["max"] = None
                    item["mean"] = 0
                    item["std"] = 0
                    item.pop("sum")
                    item.pop("sum_square")
                else:
                    item["mean"] = (item["n_overi36"] * max_i36 + item["sum"]) / item[
                        "size"
                    ]
                    nsum_square = item["n_overi36"] * (max_i36**2) + item.pop(
                        "sum_square"
                    )
                    nsum = item["n_overi36"] * max_i36 + item.pop("sum")
                    item["std"] = math.sqrt(
                        nsum_square / item["size"] - (nsum / item["size"]) ** 2
                    )
            return pinfo

        get_spark_context().textFile(raw_quantity_values_file).map(orjson.loads).map(
            lambda x: (x[0], create_stats(x[1]))
        ).reduceByKey(compute_statistic).map(itemgetter(1)).map(
            compute_statistic_postprocess
        ).map(
            lambda x: orjson.dumps(
                x, option=orjson.OPT_SERIALIZE_DATACLASS, default=list
            )
        ).coalesce(
            8
        ).saveAsTextFile(
            quantity_stats_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )


if __name__ == "__main__":
    # wikidata_quantity_prop_stats()
    qnodes_identifier()
    exit()

    # rdd = qnodes()
    # values = rdd.filter(lambda x: x['id'] == "L252247-F2").take(1)
    # print(orjson.dumps(values[0]).decode())
    wiki_article_to_qnode(lang="en")
    # rdd = qnodes_en()
    # # print(rdd.count())
    # # values = rdd.take(1)
    # values = rdd.filter(lambda x: x.id == "Q3034572").take(1)
    # print(values)
    exit(0)

    # rdd = qnodes()
    # values = rdd.filter(lambda x: x['id'] == "Q2188143").take(1)
    # print(values)

    import json

    # wdclasses = WDClass.from_file()
    # wdprops = WDProperty.from_file()
    #
    # # wikidata_graph_structure(rdd)
    # rdd = wikidata_schema()
    #     schema = rdd.filter(lambda x: x[1]['id'] == 'Q12140').collect()[0][1]

    # clbl = lambda x: f"{wdclasses[x].label} ({x})"

    # info = {
    #     "id": schema['id'],
    #     "label": wdclasses[schema['id']].label,
    #     "n_instances": schema['n_instances'],
    #     "props": {}
    # }
    # for prop, prop_schema in sorted(schema['props'].items(), key=lambda x: x[1]['n_stmts'], reverse=True):
    #     lbl = f"{wdprops[prop].label} ({prop})"
    #     info['props'][lbl] = prop_schema
    #     for c in prop_schema['classes']

    # with open("/nas/home/binhvu/workspace/sm-dev/sm-unknown/test.json", "w") as f:
    #     f.write(json.dumps(info, indent=4))
    #     exit(0)
    # from sm_unk.prelude import M
    # M.serialize_json(rdd.filter(lambda x: x['id'] == 'Q5').take(1)[0], WIKIDATA_DIR + "/test.json", indent=4)

    # def norm(args):
    #     qnode_id, schema = args
    #     for prop, prop_schema in schema['props'].items():
    #         norm = {
    #             'prob': prop_schema['n_instances'] / schema['n_instances'],
    #             'avg_n_stmts': prop_schema['n_stmts'] / prop_schema['n_instances'],
    #             # somehow we have statements that don't have any values
    #             'prob_value_object': prop_schema['f_so_ip'] / max(prop_schema['f_so_ip'] + prop_schema['f_sd_ip'], 1),
    #             'prob_value_literal': prop_schema['f_sd_ip'] / max(prop_schema['f_so_ip'] + prop_schema['f_sd_ip'], 1),
    #             'prob_class_value': {},
    #             'prob_datatype_value': {},
    #             'qualifiers': {},
    #             'examples': prop_schema['examples']
    #         }

    #         for k, v in prop_schema['f_s_ipc'].items():
    #             norm['prob_class_value'][k] = v / prop_schema['f_so_ip']
    #         for k, v in prop_schema['f_s_ipd'].items():
    #             norm['prob_datatype_value'][k] = v / prop_schema['f_sd_ip']
    #         for qual, qual_schema in prop_schema['qualifiers'].items():
    #             norm['qualifiers'][qual] = {
    #                 'avg_n_qualifiers': qual_schema['g_q_ipq'] / qual_schema['g_s_ipq'],
    #                 'prob': qual_schema['g_s_ipq'] / (prop_schema['f_so_ip'] + prop_schema['f_sd_ip']),
    #                 'prob_value_object': qual_schema['g_qo_ipq'] / (qual_schema['g_qo_ipq'] + qual_schema['g_qd_ipq']),
    #                 'prob_value_literal': qual_schema['g_qd_ipq'] / (qual_schema['g_qo_ipq'] + qual_schema['g_qd_ipq']),
    #                 'prob_class_value': {},
    #                 'prob_datatype_value': {},
    #             }

    #             for k, v in qual_schema['g_q_ipqc'].items():
    #                 norm['qualifiers'][qual]['prob_class_value'][k] = v / qual_schema['g_qo_ipq']
    #             for k, v in qual_schema['g_q_ipqd'].items():
    #                 norm['qualifiers'][qual]['prob_datatype_value'][k] = v / qual_schema['g_qd_ipq']

    #         schema['props'][prop] = norm
    #     return schema

    # resp1 = rdd2.map(itemgetter(1)).filter(lambda x: x['id'] == 'Q15991303').take(1)[0]
    # resp2 = rdd.filter(lambda x: x['id'] == 'Q15991303').take(1)[0]

    # M.serialize_json(resp1, WIKIDATA_DIR + "/test.json", indent=4)
    # M.serialize_json(norm((1, resp1)), WIKIDATA_DIR + "/test1.json", indent=4)
    # M.serialize_json(resp2, WIKIDATA_DIR + "/test2.json", indent=4)

    # print(rdd.take(1))

    # def test(qnode):
    #     for p, vals in qnode.props.items():
    #         if p in {'P3036'}:
    #             return True
    #     return False

    # result = rdd.filter(test).map(lambda x: x.id).distinct().take(1000)
    # print(result)

    # rdd = wikidata_wikipedia_links()
    # print(rdd.filter(lambda x: x['id'] == 'Q6323219').collect())
    # rdd = wiki_reltables_instances()
    # print(rdd.filter(lambda x: x['id'] == 'Q885507').take(1))

    # rdd = wikidata_en_properties()
    # rdd = wikidata_en_ontology()
    # print(rdd.count())
    # input(">>> Press any key to finish")
