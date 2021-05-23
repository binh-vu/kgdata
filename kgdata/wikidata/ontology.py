import os
from operator import add, itemgetter
from typing import Dict, Set

import networkx as nx
import orjson
from tqdm.auto import tqdm

import kgdata.misc as M
from kgdata.config import WIKIDATA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.rdd_datasets import qnodes_en
from kgdata.wikidata.wikidatamodels import QNode, WDClass, WDProperty

"""
This module provides function to extract Wikidata ontology from its dump. 
"""


def make_ontology(outfile: str = os.path.join(WIKIDATA_DIR, "ontology")):
    """Get the list of classes and predicates from Wikidata
    See the data structure: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Site_Links
    """
    sc = get_spark_context()

    # list of class and prop ids
    step0_file = os.path.join(outfile, "step_0")
    # list of class and prop full qnode
    step1_file = os.path.join(outfile, "step_1")
    # count the frequency of props per class
    step2_file = os.path.join(outfile, "freq_stats.jl")

    class_qnode_file = os.path.join(outfile, "classes.jl")
    prop_qnode_file = os.path.join(outfile, "properties.jl")

    if not does_result_dir_exist(step0_file):

        def p_0_get_property_and_class(qnode: QNode):
            if qnode.type == "property":
                qnode_ids = [stmt.value.as_qnode_id() for stmt in qnode.props.get("P1647", [])]
                qnode_ids.append(qnode.id)
                return qnode_ids

            if "P279" in qnode.props:
                # this qnode is a class, but we want to get the parent classes as well.
                qnode_ids = [stmt.value.as_qnode_id() for stmt in qnode.props.get("P279", [])]
                qnode_ids.append(qnode.id)
                return qnode_ids

            return [stmt.value.as_qnode_id() for stmt in qnode.props.get("P31", [])]

        qnodes_en().flatMap(p_0_get_property_and_class).distinct().coalesce(
            256
        ).saveAsTextFile(
            step0_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    if not does_result_dir_exist(step1_file):
        qnode_ids = sc.broadcast(set(sc.textFile(step0_file).collect()))
        qnodes_en().filter(lambda qnode: qnode.id in qnode_ids.value).map(
            QNode.serialize
        ).saveAsTextFile(
            step1_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    item_rdd = sc.textFile(step1_file).map(QNode.deserialize)

    if not os.path.exists(class_qnode_file):

        def p_1_extract_class(qnode: QNode):
            if qnode.type != "item":
                return None

            return {
                "id": qnode.id,
                "label": qnode.label,
                "description": qnode.description,
                "datatype": qnode.datatype,
                "aliases": qnode.aliases,
                "parents": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P279", [])}
                ),
                "properties": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1963", [])}
                ),
                "different_froms": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1889", [])}
                ),
                "equivalent_classes": sorted(
                    {stmt.value.as_string() for stmt in qnode.props.get("P1709", [])}
                ),
            }

        class_rdd = (
            item_rdd.map(p_1_extract_class)
                .filter(lambda x: x is not None)
                .map(orjson.dumps)
        )
        saveAsSingleTextFile(class_rdd, class_qnode_file)

    if not os.path.exists(prop_qnode_file):

        def p_2_extract_property(qnode: QNode):
            if qnode.type != "property":
                return None
            return {
                "id": qnode.id,
                "label": qnode.label,
                "description": qnode.description,
                "datatype": qnode.datatype,
                "aliases": qnode.aliases,
                "parents": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1647", [])}
                ),
                "see_also": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1659", [])}
                ),
                "equivalent_properties": sorted(
                    {stmt.value.as_string() for stmt in qnode.props.get("P1628", [])}
                ),
                "subjects": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1629", [])}
                ),
                "inverse_properties": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P1696", [])}
                ),
                "instanceof": sorted(
                    {stmt.value.as_qnode_id() for stmt in qnode.props.get("P31", [])}
                ),
            }

        prop_rdd = (
            item_rdd.map(p_2_extract_property)
                .filter(lambda x: x is not None)
                .map(orjson.dumps)
        )
        saveAsSingleTextFile(prop_rdd, prop_qnode_file)

    if not does_result_dir_exist(step2_file):

        def p_3_count_props(qnode: QNode):
            if qnode.type == "property":
                return []
            class_ids = []
            for stmt in qnode.props.get("P31", []):
                class_ids.append(stmt.value.as_qnode_id())

            triples = []
            for p in qnode.props:
                if p == "P31":
                    continue
                for cid in class_ids:
                    triples.append(((cid, p), 1))
            for cid in class_ids:
                triples.append(((cid, "_ccount"), 1))
            return triples

        sp_count = qnodes_en().flatMap(p_3_count_props).reduceByKey(add).collect()
        class_freq = {}
        for (s, p), c in sp_count:
            if s not in class_freq:
                class_freq[s] = {"props": {}, "count": 0, "class": s}
            if p == "_ccount":
                class_freq[s]["count"] = c
            else:
                class_freq[s]["props"][p] = c

        M.serialize_jl(sorted(class_freq.values(), key=itemgetter("class")), step2_file)


def make_superclass_closure(outdir: str = os.path.join(WIKIDATA_DIR, "ontology")):
    def get_all_parents(index: Dict[str, dict], id: str, parents: Set[str]):
        for pid in index[id]["parents"]:
            if pid not in parents:
                if pid not in index:
                    continue
                parents.add(pid)
                get_all_parents(index, pid, parents)

    classes = M.deserialize_jl(os.path.join(outdir, "classes.jl"))
    classes = {c["id"]: c for c in classes}

    class_closure = {}
    for c in tqdm(classes.values()):
        parents = set()
        get_all_parents(classes, c["id"], parents)
        class_closure[c["id"]] = list(parents)

    M.serialize_json(class_closure, os.path.join(outdir, "superclasses_closure.json"))

    predicates = M.deserialize_jl(os.path.join(outdir, "properties.jl"))
    predicates = {p["id"]: p for p in predicates}

    predicate_closure = {}
    for r in tqdm(predicates.values()):
        parents = set()
        get_all_parents(predicates, r["id"], parents)
        predicate_closure[r["id"]] = list(parents)

    M.serialize_json(
        predicate_closure, os.path.join(outdir, "superproperties_closure.json")
    )


def examine_ontology_property(indir: str = os.path.join(WIKIDATA_DIR, "ontology")):
    classes = WDClass.from_file(indir)

    g = nx.DiGraph()
    for c in tqdm(classes.values()):
        for pcid in c.parents:
            g.add_edge(c.id, pcid)

    # cycles = list(nx.simple_cycles(g))
    # print(f"Detect {len(cycles)} cycles in the ontology classes")
    # for cycle in cycles:
    #     print("\t> ", cycle)

    predicates = WDProperty.from_file(indir)

    g = nx.DiGraph()
    for p in tqdm(predicates.values()):
        for ppid in p.parents:
            g.add_edge(p.id, ppid)

    cycles = list(nx.simple_cycles(g))
    print(f"Detect {len(cycles)} cycles in the ontology predicates")
    for cycle in cycles:
        print("\t> ", cycle)


def save_ontology_to_db(in_and_out_dir: str = os.path.join(WIKIDATA_DIR, "ontology")):
    import rocksdb

    wdclasses = WDClass.from_file(in_and_out_dir, load_parent_closure=True)
    # wdprops = WDProperty.from_file(in_and_out_dir, load_parent_closure=True)
    # assert len(set(wdclasses.keys()).intersection(wdprops.keys())) == 0

    db = rocksdb.DB(os.path.join(in_and_out_dir, "wdclasses.db"), rocksdb.Options(create_if_missing=True))
    wb = rocksdb.WriteBatch()
    for id, item in tqdm(wdclasses.items(), total=len(wdclasses)):
        wb.put(id.encode(), item.serialize())
    db.write(wb)


if __name__ == "__main__":
    # make_ontology()
    # make_superclass_closure()
    # examine_ontology_property()
    save_ontology_to_db()
