from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import orjson
from rdflib import OWL, RDF, RDFS, URIRef

from kgdata.dataset import Dataset
from kgdata.db import ser_to_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.misc.ntriples_parser import Triple, ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.splitter import split_a_file, split_a_list

rdf_type = str(RDF.type)
rdfs_label = str(RDFS.label)
rdfs_comment = str(RDFS.comment)
rdfs_subpropertyof = str(RDFS.subPropertyOf)
rdfs_subclassof = str(RDFS.subClassOf)
rdfs_domain = str(RDFS.domain)
rdfs_range = str(RDFS.range)
owl_class = str(OWL.Class)
owl_disjointwith = str(OWL.disjointWith)
owl_thing = str(OWL.Thing)


def ontology_dump() -> Dataset[RDFResource]:
    """
    Process DBpedia ontology dump:
    - normalize the data
    - fix broken references

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()

    if not does_result_dir_exist(cfg.ontology_dump / "step2"):
        infile = cfg.get_ontology_dump_file()
        step1_dir = cfg.ontology_dump / "step1"
        split_a_file(
            infile=infile,
            outfile=step1_dir / f"part{infile.suffix}.gz",
            n_writers=8,
            override=False,
            n_records_per_file=5000,
        )

        (
            get_spark_context()
            .textFile(str(step1_dir / "*.gz"))
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .coalesce(16)
            .saveAsTextFile(
                str(cfg.ontology_dump / "step2"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.ontology_dump / "step3"):
        ds = Dataset.string(file_pattern=str(cfg.ontology_dump / "step2" / "*.gz")).map(
            RDFResource.deser
        )

        # fix broken references
        resources = ds.get_list()

        classes = {r.id: r for r in resources if is_class(r)}
        props = {r.id: r for r in resources if is_prop(r)}

        logs = {}

        for p in props.values():
            if rdfs_subpropertyof in p.props:
                p.props[rdfs_subpropertyof] = norm_uris(
                    p.id,
                    p.props[rdfs_subpropertyof],
                    props,
                    logs,
                    norm_invalid_prop_uri,
                    ignore_out_ns=True,
                )
            for name in [rdfs_domain, rdfs_range]:
                if name in p.props:
                    p.props[name] = norm_uris(
                        p.id,
                        p.props[name],
                        classes,
                        logs,
                        norm_invalid_class_uri,
                        ignore_out_ns=False,
                    )

        for c in classes.values():
            if rdfs_subclassof in c.props:
                c.props[rdfs_subclassof] = norm_uris(
                    c.id,
                    c.props[rdfs_subclassof],
                    classes,
                    logs,
                    norm_invalid_class_uri,
                    ignore_out_ns=True,
                )

        split_a_list(
            [r.ser() for r in resources],
            cfg.ontology_dump / "step3/part.jl.gz",
            n_records_per_file=math.ceil(len(resources) / 16),
        )
        (cfg.ontology_dump / "fix.log").write_text(
            "\n".join(
                [line for id, lines in logs.items() for line in ["* " + id] + lines]
            )
        )
        (cfg.ontology_dump / "step3/_SUCCESS").touch()

    ds = Dataset.string(file_pattern=str(cfg.ontology_dump / "step3" / "*.gz")).map(
        RDFResource.deser
    )
    if not (cfg.ontology_dump / "predicates.txt").exists():
        saveAsSingleTextFile(
            ds.get_rdd().flatMap(lambda x: x.props.keys()).distinct(),
            cfg.ontology_dump / "predicates.txt",
        )

    return ds


def aggregated_triples(val: tuple[str, Iterable[Triple]]) -> RDFResource:
    source, pred_objs = val
    props: dict[str, list] = defaultdict(list)
    for _, pred, obj in pred_objs:
        props[str(pred)].append(obj)
    return RDFResource(source, dict(props))


def is_target_ontologies(uri: str) -> bool:
    return uri.startswith("http://dbpedia.org/ontology/") or any(
        uri.startswith(basens) for basens in [str(RDF), str(RDFS), str(OWL)]
    )


def should_norm_uri(uri: str) -> bool:
    return uri.startswith("http://dbpedia.org/ontology/")


def is_class(resource: RDFResource) -> bool:
    return (
        OWL.Class in resource.props.get(rdf_type, [])
        or rdfs_subclassof in resource.props
    )


def is_prop(resource: RDFResource) -> bool:
    return (
        RDF.Property in resource.props.get(rdf_type, [])
        or rdfs_subpropertyof in resource.props
    )


def norm_uris(
    id: str,
    lst: list[URIRef],
    ids: dict[str, Any] | set[str],
    logs: dict[str, list[str]],
    norm_invalid_uri: Callable[[str], str],
    ignore_out_ns: bool = True,
) -> list[URIRef]:
    out = []
    for uri in lst:
        uri_s = str(uri)
        if not is_target_ontologies(uri_s):
            if not ignore_out_ns:
                out.append(uri)
            continue
        if not should_norm_uri(uri):
            out.append(uri)
            continue
        if uri_s not in ids:
            norm_uri = norm_invalid_uri(uri_s)
            if norm_uri in ids or norm_uri == owl_thing:
                if id not in logs:
                    logs[id] = []
                logs[id].append(f"\t- Convert '{uri_s}' to '{norm_uri}'")
                out.append(URIRef(norm_uri))
            else:
                if id not in logs:
                    logs[id] = []
                logs[id].append(f"\t- Missing '{uri_s}'")
        else:
            out.append(uri)
    return out


def norm_invalid_prop_uri(uri: str) -> str:
    if uri == "http://dbpedia.org/ontology/Thing":
        return owl_thing
    prefix, name = uri.rsplit("/", 1)
    return prefix + "/" + name[0].lower() + name[1:]


def norm_invalid_class_uri(uri: str) -> str:
    if uri == "http://dbpedia.org/ontology/Thing":
        return owl_thing
    prefix, name = uri.rsplit("/", 1)
    return prefix + "/" + name[0].upper() + name[1:]
