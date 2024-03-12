from __future__ import annotations

import math
import re
from collections import defaultdict
from functools import lru_cache
from typing import Any, Callable, Iterable

import serde.csv
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.misc.ntriples_parser import Triple, ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import ExtendedRDD
from kgdata.splitter import split_a_file, split_a_list
from rdflib import OWL, RDF, RDFS, BNode, URIRef

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


@lru_cache()
def ontology_dump() -> Dataset[RDFResource]:
    """
    Process DBpedia ontology dump:
    - normalize the data
    - fix broken references

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()

    infile = cfg.get_ontology_dump_file()
    (dump_date,) = re.findall(r"\d{8}", infile.name)

    step2_ds = Dataset(
        cfg.ontology_dump / "step2" / "*.gz",
        RDFResource.deser,
        name=f"ontology-dump/{dump_date}/step-2",
        dependencies=[],
    )
    final_ds = Dataset(
        cfg.ontology_dump / "step3" / "*.gz",
        RDFResource.deser,
        name=f"ontology-dump/{dump_date}/final",
        dependencies=[step2_ds],
    )

    if not step2_ds.has_complete_data():
        step1_dir = cfg.ontology_dump / "step1"
        split_a_file(
            infile=infile,
            outfile=step1_dir / f"part{infile.suffix}.gz",
            n_writers=8,
            override=False,
            n_records_per_file=5000,
        )

        (
            ExtendedRDD.textFile(step1_dir / "*.gz")
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .auto_coalesce(cache=True)
            .save_like_dataset(
                step2_ds,
                checksum=False,
            )
        )

    if not final_ds.has_complete_data():
        # fix broken references
        resources = step2_ds.get_list()

        # remove resources that have been redirected
        redirected_resources = {
            x[0] for x in serde.csv.deser(cfg.get_redirection_modified_file())
        }
        resources = [r for r in resources if r.id not in redirected_resources]

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
        assert final_ds.name is not None
        final_ds.sign(final_ds.name, [step2_ds])

    if not (cfg.ontology_dump / "predicates.txt").exists():
        (
            final_ds.get_extended_rdd()
            .flatMap(lambda x: x.props.keys())
            .distinct()
            .save_as_single_text_file(
                cfg.ontology_dump / "predicates.txt",
            )
        )

    return final_ds


def aggregated_triples(val: tuple[URIRef | BNode, Iterable[Triple]]) -> RDFResource:
    return RDFResource.from_triples(val[0], val[1])


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
