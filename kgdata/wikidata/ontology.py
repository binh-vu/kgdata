from functools import partial
import os, gzip
from operator import add, itemgetter
from pathlib import Path
from typing import Dict, Optional, Set, Union
from graph.interface import BaseEdge, BaseNode
from hugedict.misc import identity

import rocksdb
from loguru import logger
from tqdm.auto import tqdm

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.wikidata.models import QNode, WDClass, WDProperty
from kgdata.wikidata.rdd_datasets import qnodes_en
from sm.misc import serialize_jl, serialize_json, deserialize_jl
from graph.retworkx import digraph_find_cycle, RetworkXStrDiGraph

"""
This module provides function to extract Wikidata ontology from its dump. 
"""


def make_ontology(outfile: str = os.path.join(WIKIDATA_DIR, "ontology")):
    """Get the list of classes and predicates from Wikidata
    See the data structure: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Site_Links
    """
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
                qnode_ids = [
                    stmt.value.as_entity_id() for stmt in qnode.props.get("P1647", [])
                ]
                qnode_ids.append(qnode.id)
                return qnode_ids

            if "P279" in qnode.props:
                # this qnode is a class, but we want to get the parent classes as well.
                qnode_ids = [
                    stmt.value.as_entity_id() for stmt in qnode.props.get("P279", [])
                ]
                qnode_ids.append(qnode.id)
                return qnode_ids

            return [stmt.value.as_entity_id() for stmt in qnode.props.get("P31", [])]

        qnodes_en().flatMap(p_0_get_property_and_class).distinct().coalesce(
            256
        ).saveAsTextFile(
            step0_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    if not does_result_dir_exist(step1_file):
        sc = get_spark_context()

        qnode_ids = sc.broadcast(set(sc.textFile(step0_file).collect()))
        qnodes_en().filter(lambda qnode: qnode.id in qnode_ids.value).map(
            QNode.serialize
        ).saveAsTextFile(
            step1_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    item_rdd = lambda: get_spark_context().textFile(step1_file).map(QNode.deserialize)

    if not os.path.exists(class_qnode_file):

        def p_1_extract_class(qnode: QNode):
            if qnode.type != "item":
                return None

            return WDClass(
                id=qnode.id,
                label=qnode.label,
                description=qnode.description,
                datatype=qnode.datatype,
                aliases=qnode.aliases,
                parents=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P279", [])}
                ),
                properties=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1963", [])}
                ),
                different_froms=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1889", [])}
                ),
                equivalent_classes=sorted(
                    {stmt.value.as_string() for stmt in qnode.props.get("P1709", [])}
                ),
                parents_closure=set(),
            )

        class_rdd = (
            item_rdd()
            .map(p_1_extract_class)
            .filter(lambda x: x is not None)
            .map(WDClass.serialize)
        )
        saveAsSingleTextFile(class_rdd, class_qnode_file)

    if not os.path.exists(prop_qnode_file):

        def p_2_extract_property(qnode: QNode):
            if qnode.type != "property":
                return None
            return WDProperty(
                id=qnode.id,
                label=qnode.label,
                description=qnode.description,
                datatype=qnode.datatype,
                aliases=qnode.aliases,
                parents=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1647", [])}
                ),
                see_also=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1659", [])}
                ),
                equivalent_properties=sorted(
                    {stmt.value.as_string() for stmt in qnode.props.get("P1628", [])}
                ),
                subjects=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1629", [])}
                ),
                inverse_properties=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P1696", [])}
                ),
                instanceof=sorted(
                    {stmt.value.as_entity_id() for stmt in qnode.props.get("P31", [])}
                ),
            )

        prop_rdd = (
            item_rdd()
            .map(p_2_extract_property)
            .filter(lambda x: x is not None)
            .map(WDProperty.serialize)
        )
        saveAsSingleTextFile(prop_rdd, prop_qnode_file)

    if not os.path.exists(step2_file):

        def p_3_count_props(qnode: QNode):
            if qnode.type == "property":
                return []
            class_ids = []
            for stmt in qnode.props.get("P31", []):
                class_ids.append(stmt.value.as_entity_id())

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

        serialize_jl(sorted(class_freq.values(), key=itemgetter("class")), step2_file)


def make_superclass_closure(outdir: str = os.path.join(WIKIDATA_DIR, "ontology")):
    def get_all_parents(index: Dict[str, dict], id: str, parents: Set[str]):
        for pid in index[id]["parents"]:
            if pid not in parents:
                if pid not in index:
                    continue
                parents.add(pid)
                get_all_parents(index, pid, parents)

    superclasses_closure_file = os.path.join(outdir, "superclasses_closure.jl")
    superproperties_closure_file = os.path.join(outdir, "superproperties_closure.jl")

    if not os.path.exists(superclasses_closure_file):
        logger.info("Make superclass closure...")
        # need to make sure that they are in the same order
        classes = deserialize_jl(os.path.join(outdir, "classes.jl"))
        classes_order = [c["id"] for c in classes]
        classes = {c["id"]: c for c in classes}

        class_closure = {}
        for c in tqdm(classes.values()):
            parents = set()
            get_all_parents(classes, c["id"], parents)
            class_closure[c["id"]] = list(parents)

        class_closure = [(cid, class_closure[cid]) for cid in classes_order]
        serialize_jl(class_closure, superclasses_closure_file)

    if not os.path.exists(superproperties_closure_file):
        logger.info("Make superproperties closure...")
        predicates = deserialize_jl(os.path.join(outdir, "properties.jl"))
        predicates_order = [p["id"] for p in predicates]
        predicates = {p["id"]: p for p in predicates}

        predicate_closure = {}
        for r in tqdm(predicates.values()):
            parents = set()
            get_all_parents(predicates, r["id"], parents)
            predicate_closure[r["id"]] = list(parents)

        predicate_closure = [(pid, predicate_closure[pid]) for pid in predicates_order]
        serialize_jl(predicate_closure, superproperties_closure_file)


def examine_ontology_property(indir: str = os.path.join(WIKIDATA_DIR, "ontology")):
    def print_cycle(edges: list[BaseEdge[str, str]]):
        print("\t>", " -> ".join([e.source for e in edges]) + " -> " + edges[-1].target)

    def find_cycles(g: RetworkXStrDiGraph[str, BaseNode, BaseEdge[str, str]]):
        nodes = g.nodes()
        for node in nodes:
            cycle = digraph_find_cycle(g, node.id)
            if len(cycle) == 0:
                continue

            while True:
                print_cycle(cycle)
                e = cycle[0]
                g.remove_edges_between_nodes(e.source, e.target)
                cycle = digraph_find_cycle(g, cycle[-1].target)
                if len(cycle) == 0:
                    break

    # classes = WDClass.from_file(indir)
    # g: RetworkXStrDiGraph[str, BaseNode, BaseEdge[str, str]] = RetworkXStrDiGraph(
    #     multigraph=False
    # )
    # for c in tqdm(classes.values()):
    #     for pcid in c.parents:
    #         g.add_edge(BaseEdge(-1, c.id, pcid, ""))

    # print("Detecting cycles in the ontology classes...")
    # find_cycles(g)

    predicates = WDProperty.from_file(indir)

    g: RetworkXStrDiGraph[str, BaseNode, BaseEdge[str, str]] = RetworkXStrDiGraph(
        multigraph=False
    )
    for p in tqdm(predicates.values()):
        for ppid in p.parents:
            g.add_edge(BaseEdge(-1, p.id, ppid, ""))

    print("Detecting cycles in the ontology properties...")
    find_cycles(g)


def save_wdprops(indir: Union[str, Path], db):
    if indir == "":
        indir = os.path.join(WIKIDATA_DIR, "ontology")

    wdprops = WDProperty.from_file(indir, load_parent_closure=True)
    wb = rocksdb.WriteBatch()
    for id, item in tqdm(wdprops.items(), total=len(wdprops)):
        wb.put(id.encode(), item.serialize())
    db.write(wb)


def save_wdclasses(indir: Union[str, Path], db):
    if indir == "":
        indir = os.path.join(WIKIDATA_DIR, "ontology")

    wb = rocksdb.WriteBatch()
    for idx, item in tqdm(
        enumerate(WDClass.iter_file(indir, load_parent_closure=True)),
        desc="Write WDClass",
    ):
        wb.put(item.id.encode(), item.serialize())
        if idx % 1000 == 0:
            db.write(wb)
            wb = rocksdb.WriteBatch()
    db.write(wb)


if __name__ == "__main__":
    examine_ontology_property()
