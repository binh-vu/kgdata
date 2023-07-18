from __future__ import annotations

import re
from urllib.parse import urlparse

import orjson
from rdflib import OWL, RDF, RDFS, BNode, Literal, URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDataDirCfg
from kgdata.dbpedia.datasets.ontology_dump import RDFResource, ontology_dump
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from kgdata.models.ont_class import OntologyClass
from kgdata.models.ont_property import OntologyProperty
from kgdata.spark import does_result_dir_exist
from sm.misc.funcs import assert_not_null

rdf_type = str(RDF.type)
rdfs_label = str(RDFS.label)
rdfs_comment = str(RDFS.comment)
rdfs_subpropertyof = str(RDFS.subPropertyOf)


def properties() -> Dataset[OntologyProperty]:
    cfg = DBpediaDataDirCfg.get_instance()
    outdir = cfg.properties

    if not does_result_dir_exist(outdir):
        (
            ontology_dump()
            .get_rdd()
            .filter(is_prop)
            .map(to_prop)
            .map(lambda c: orjson.dumps(c.to_dict()))
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        outdir / "*.gz",
        deserialize=lambda x: OntologyProperty.from_dict(orjson.loads(x)),
    )


def is_prop(resource: RDFResource) -> bool:
    return RDF.Property in resource.props.get(rdf_type, [])


def to_prop(resource: RDFResource, default_lang: str = "en") -> OntologyProperty:
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

    return OntologyProperty(
        id=resource.id,
        label=label,
        description=description,
        aliases=MultiLingualStringList({default_lang: []}, default_lang),
        parents=[str(term) for term in resource.props.get(rdfs_subpropertyof, [])],
        datatype=extract_datatype(resource),
        related_properties=[],
        equivalent_properties=[
            str(term) for term in resource.props.get(str(OWL.equivalentProperty), [])
        ],
        subjects=[str(term) for term in resource.props.get(str(RDFS.domain), [])],
        inverse_properties=[],
        instanceof=[str(term) for term in resource.props.get(rdf_type, [])],
        ancestors=set(),
    )


def assert_all_literal(terms: list[URIRef | Literal | BNode]) -> list[Literal]:
    assert all(isinstance(term, Literal) for term in terms)
    return terms  # type: ignore


def as_multilingual(terms: list[Literal], default_lang: str = "en"):
    if len(terms) == 0:
        return MultiLingualString({default_lang: ""}, default_lang)

    lang2value: dict[str, str] = {
        assert_not_null(term.language): term.value for term in terms
    }
    return MultiLingualString(lang2value, next(iter(lang2value.keys())))


def extract_datatype(resource: RDFResource) -> DataType:
    if OWL.ObjectProperty in resource.props.get(str(RDF.type), []):
        return "entity"

    ranges = resource.props.get(str(RDFS.range), [])
    if len(ranges) > 0:
        assert len(ranges) == 1
        return ranges[0]

    assert False, resource.id


ID_TO_LANGUAGE = {
    "http://dbpedia.org/ontology/ونٹر_اسپورٹ_پلیئر۔": "ur",
    "http://dbpedia.org/ontology/currentSeason": "en",
}


def extract_label(resource: RDFResource) -> MultiLingualString:
    """Extract label of a resource and guess its default language."""
    default_label = urlparse(resource.id).path.split("/ontology/", 1)[1]
    default_label = re.sub(
        r"((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))", r" \1", default_label
    ).lower()
    if rdfs_label in resource.props:
        label = as_multilingual(assert_all_literal(resource.props[rdfs_label]))
    else:
        default_lang = ID_TO_LANGUAGE[resource.id]
        label = MultiLingualString({default_lang: default_label}, default_lang)

    if len(label.lang2value) == 1:
        # to handle case where URI != default label. https://dbpedia.org/ontology/Academic
        default_lang = label.lang
    else:
        match_langs = []
        for lang, value in label.lang2value.items():
            if value.lower() == default_label:
                match_langs.append(lang)
        if len(match_langs) == 1:
            default_lang = match_langs[0]
        else:
            if "en" in match_langs:
                default_lang = "en"
            elif "en" in label.lang2value:
                default_lang = "en"
            elif resource.id in ID_TO_LANGUAGE:
                default_lang = ID_TO_LANGUAGE[resource.id]
            else:
                raise NotImplementedError(resource.id)
        label.lang = default_lang

    return label
