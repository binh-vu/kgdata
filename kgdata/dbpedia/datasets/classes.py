from __future__ import annotations

from functools import partial

from rdflib import OWL, RDFS

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.ontology_dump import RDFResource, ontology_dump
from kgdata.dbpedia.datasets.properties import (
    as_multilingual,
    assert_all_literal,
    extract_label,
    is_prop,
    rdf_type,
    rdfs_comment,
)
from kgdata.misc.hierarchy import build_ancestors
from kgdata.models.multilingual import MultiLingualStringList
from kgdata.models.ont_class import OntologyClass, get_default_classes
from kgdata.splitter import split_a_list

rdfs_subclassof = str(RDFS.subClassOf)
owl_class = str(OWL.Class)
owl_disjointwith = str(OWL.disjointWith)


def classes() -> Dataset[OntologyClass]:
    cfg = DBpediaDirCfg.get_instance()
    ds = Dataset(
        cfg.classes / "*.jl",
        partial(deser_from_dict, OntologyClass),
        name="classes",
        dependencies=[ontology_dump()],
    )

    if not ds.has_complete_data():
        ont_ds = ontology_dump()
        ont_ds_rdd = ont_ds.get_rdd_alike()

        domain2props = (
            ont_ds_rdd.filter(is_prop)
            .flatMap(
                lambda r: [
                    (str(uri), r.id) for uri in r.props.get(str(RDFS.domain), [])
                ]
            )
            .groupByKey()
        )

        def merge_domains(g):
            ontcls, props = g[1]
            if props is not None:
                ontcls.properties = list(props)
            return ontcls

        classes = (
            ont_ds_rdd.filter(is_class)
            .map(to_class)
            .union(get_default_classes())
            .map(lambda x: (x.id, x))
            .leftOuterJoin(domain2props)
            .map(merge_domains)
            .collect()
        )
        build_ancestors(classes)
        split_a_list([ser_to_dict(c) for c in classes], cfg.classes / "part.jl")
        (cfg.classes / "_SUCCESS").touch()
        ds.sign("classes", [ont_ds])

    return ds


def is_class(resource: RDFResource) -> bool:
    return (
        OWL.Class in resource.props.get(rdf_type, [])
        or rdfs_subclassof in resource.props
    )


def to_class(resource: RDFResource) -> OntologyClass:
    label = extract_label(resource)
    default_lang = label.lang

    description = as_multilingual(
        assert_all_literal(resource.props.get(rdfs_comment, []))
    )
    if len(description.lang2value) > 1:
        if default_lang in description.lang2value:
            description.lang = default_lang
        else:
            # to handle case where there is no description in default language
            description.lang = default_lang
            description.lang2value[default_lang] = ""

    return OntologyClass(
        id=resource.id,
        label=label,
        description=description,
        aliases=MultiLingualStringList({default_lang: []}, default_lang),
        parents=[str(term) for term in resource.props.get(rdfs_subclassof, [])],
        properties=[],
        different_froms=[
            str(term) for term in resource.props.get(owl_disjointwith, [])
        ],
        equivalent_classes=[
            str(term) for term in resource.props.get(str(OWL.equivalentClass), [])
        ],
        ancestors={},
    )
