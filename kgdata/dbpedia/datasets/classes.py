from __future__ import annotations

import re
from urllib.parse import urlparse

import orjson
from rdflib import OWL, RDF, RDFS, BNode, Literal, URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDataDirCfg
from kgdata.dbpedia.datasets.ontology_dump import RDFResource, ontology_dump
from kgdata.dbpedia.datasets.properties import (
    as_multilingual,
    assert_all_literal,
    extract_label,
    is_prop,
    rdf_type,
    rdfs_comment,
)
from kgdata.models.multilingual import MultiLingualStringList
from kgdata.models.ont_class import OntologyClass
from kgdata.spark import does_result_dir_exist

rdfs_subclassof = str(RDFS.subClassOf)
owl_class = str(OWL.Class)
owl_disjointwith = str(OWL.disjointWith)


def classes() -> Dataset[OntologyClass]:
    cfg = DBpediaDataDirCfg.get_instance()
    outdir = cfg.classes

    if not does_result_dir_exist(outdir):
        domain2props = (
            ontology_dump()
            .get_rdd()
            .filter(is_prop)
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

        (
            ontology_dump()
            .get_rdd()
            .filter(is_class)
            .map(to_class)
            .map(lambda x: (x.id, x))
            .leftOuterJoin(domain2props)
            .map(merge_domains)
            .map(lambda c: orjson.dumps(c.to_dict()))
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        outdir / "*.gz",
        deserialize=lambda x: OntologyClass.from_dict(orjson.loads(x)),
    )


def is_class(resource: RDFResource) -> bool:
    return OWL.Class in resource.props.get(rdf_type, [])


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
        ancestors=set(),
    )
