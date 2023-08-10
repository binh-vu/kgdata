from __future__ import annotations

from functools import partial
from math import ceil

import orjson
import serde.jl
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
from kgdata.models.multilingual import MultiLingualStringList
from kgdata.models.ont_class import OntologyClass, get_default_classes
from kgdata.spark import does_result_dir_exist, get_spark_context, saveAsSingleTextFile
from kgdata.splitter import split_a_list
from kgdata.wikidata.datasets.classes import build_ancestors

rdfs_subclassof = str(RDFS.subClassOf)
owl_class = str(OWL.Class)
owl_disjointwith = str(OWL.disjointWith)


def classes() -> Dataset[OntologyClass]:
    cfg = DBpediaDirCfg.get_instance()

    if not does_result_dir_exist(cfg.classes):
        ont_ds = ontology_dump().get_rdd_alike()

        domain2props = (
            ont_ds.filter(is_prop)
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
            ont_ds.filter(is_class)
            .map(to_class)
            .union(get_default_classes())
            .map(lambda x: (x.id, x))
            .leftOuterJoin(domain2props)
            .map(merge_domains)
            .collect()
        )

        id2ancestors = build_ancestors({c.id: c.parents for c in classes})
        for c in classes:
            c.ancestors = id2ancestors[c.id]

        split_a_list([ser_to_dict(c) for c in classes], cfg.classes / "part.jl")
        (cfg.classes / "_SUCCESS").touch()

    return Dataset(
        cfg.classes / "*.jl",
        deserialize=partial(deser_from_dict, OntologyClass),
    )

    # deser_cls = partial(deser_from_dict, OntologyClass)

    # if not does_result_dir_exist(cfg.classes / "ancestors"):
    #     id2parents = (
    #         Dataset(cfg.classes / "classes/*.gz", deserialize=deser_cls)
    #         .map(lambda x: (x.id, x.parents))
    #         .get_dict()
    #     )
    #     id2ancestors = build_ancestors(id2parents)
    #     split_a_list(
    #         [orjson.dumps(x) for x in sorted(id2ancestors.items())],
    #         (cfg.classes / "ancestors/part.ndjson.gz"),
    #         n_records_per_file=ceil(len(id2ancestors) / 8),
    #     )
    #     (cfg.classes / "ancestors" / "_SUCCESS").touch()

    # if not does_result_dir_exist(cfg.classes / "full_classes"):
    #     id2ancestors = Dataset(
    #         cfg.classes / "ancestors/*.gz", deserialize=orjson.loads
    #     ).get_rdd_alike()

    #     def merge_ancestors(o):
    #         id, (cls, ancestors) = o
    #         cls.ancestors = set(ancestors)
    #         return cls

    #     (
    #         Dataset(cfg.classes / "classes/*.gz", deserialize=deser_cls)
    #         .get_rdd_alike()
    #         .map(lambda x: (x.id, x))
    #         .join(id2ancestors)
    #         .map(merge_ancestors)
    #         .map(ser_to_dict)
    #         .saveAsTextFile(
    #             str(cfg.classes / "full_classes"),
    #             compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
    #         )
    #     )

    # return Dataset(
    #     cfg.classes / "full_classes/*.gz",
    #     deserialize=deser_cls,
    # )


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
        ancestors=set(),
    )
