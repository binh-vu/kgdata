from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache, partial
from typing import Iterable, Optional

from kgdata.dataset import Dataset
from kgdata.db import deser_from_dict, ser_to_dict
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.classes import classes
from kgdata.dbpedia.datasets.generic_extractor_dump import generic_extractor_dump
from kgdata.dbpedia.datasets.mapping_extractor_dump import mapping_extractor_dump
from kgdata.dbpedia.datasets.ontology_dump import ontology_dump, rdf_type
from kgdata.dbpedia.datasets.properties import (
    as_multilingual,
    assert_all_literal,
    properties,
    rdfs_comment,
    rdfs_label,
)
from kgdata.misc.resource import RDFResource, Record
from kgdata.models.entity import Entity, Statement
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from kgdata.models.ont_class import OntologyClass
from kgdata.wikipedia.misc import get_title_from_url
from rdflib import OWL, BNode, Literal, URIRef


@lru_cache()
def entities(lang: str = "en") -> Dataset[Entity]:
    cfg = DBpediaDirCfg.get_instance()

    merge_ds = Dataset(
        cfg.entities / f"merge-{lang}/*.gz",
        deserialize=partial(deser_from_dict, Entity),
        name=f"entities/merge-{lang}",
        dependencies=[
            mapping_extractor_dump(lang),
            generic_extractor_dump(lang),
        ],
    )
    final_ds = Dataset(
        cfg.entities / f"infer-{lang}/*.gz",
        deserialize=partial(deser_from_dict, Entity),
        name=f"entities/final-{lang}",
        dependencies=[merge_ds, ontology_dump()],
    )

    if not merge_ds.has_complete_data():
        part1 = mapping_extractor_dump(lang).get_extended_rdd().map(lambda r: (r.id, r))
        part2 = generic_extractor_dump(lang).get_extended_rdd().map(lambda r: (r.id, r))

        (
            part1.fullOuterJoin(part2)
            .map(lambda x: merge_resources(x[1][0], x[1][1]))
            .map(partial(to_entity, dump_lang=lang))
            .map(ser_to_dict)
            .save_like_dataset(merge_ds, checksum=False, auto_coalesce=True)
        )

    if not final_ds.has_complete_data():
        rdd = merge_ds.get_extended_rdd()
        (
            rdd.map(lambda e: (e.id, e))
            .leftOuterJoin(
                rdd.flatMap(infer_new_data).map(lambda t: (t.subject, t)).groupByKey()
            )
            .map(merge_new_triple)
            .union(
                ontology_dump()
                .get_extended_rdd()
                .map(partial(to_entity, dump_lang=lang))
            )
            .map(ser_to_dict)
            .save_like_dataset(final_ds, auto_coalesce=True, shuffle=True)
        )

    return final_ds


def merge_resources(
    r1: Optional[RDFResource], r2: Optional[RDFResource]
) -> RDFResource:
    if r1 is None:
        assert r2 is not None
        return r2
    if r2 is None:
        assert r1 is not None
        return r1
    return r1.merge(r2)


def to_entity(resource: RDFResource, dump_lang: str) -> Entity:
    label = extract_entity_label(resource, dump_lang)
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

    return Entity(
        id=resource.id,
        label=label,
        description=description,
        aliases=MultiLingualStringList({default_lang: []}, default_lang),
        props={
            pid: [to_statement(s) for s in lst] for pid, lst in resource.props.items()
        },
    )


def to_statement(value: URIRef | BNode | Literal) -> Statement:
    assert not isinstance(value, BNode)
    return Statement(value=value, qualifiers={}, qualifiers_order=[])


def extract_entity_label(
    resource: RDFResource,
    dump_lang: str = "en",
):
    """The implementation is different from extract_label from properties.py"""
    if rdfs_label in resource.props:
        label = as_multilingual(assert_all_literal(resource.props[rdfs_label]))
        if dump_lang not in label.lang2value:
            # use the URL as label
            label.lang2value[dump_lang] = get_title_from_url(resource.id, "/resource/")
        label.lang = dump_lang
    else:
        label = MultiLingualString(
            {dump_lang: get_title_from_url(resource.id, "/resource/")}, dump_lang
        )
    return label


type2contradictions = {
    URIRef(k): {URIRef(v) for v in vs}
    for k, vs in {
        "http://dbpedia.org/ontology/GovernmentType": {
            "http://dbpedia.org/ontology/Country"
        }
    }.items()
}
prop2range = {
    k: URIRef(v)
    for k, v in {
        "http://dbpedia.org/property/governmentType": "http://dbpedia.org/ontology/GovernmentType",
        "http://dbpedia.org/ontology/governmentType": "http://dbpedia.org/ontology/GovernmentType",
    }.items()
}
range_constraints = {
    URIRef(k): {URIRef(v) for v in vs}
    for k, vs in {
        "http://dbpedia.org/ontology/GovernmentType": {
            "http://dbpedia.org/ontology/Country"
        }
    }.items()
}


@dataclass
class NewTriple(Record):
    subject: str
    predicate: str
    object: URIRef | Literal
    constraints: set[URIRef | Literal] | set[URIRef] | set[Literal]


def infer_new_data(e: Entity):
    out: dict[str, NewTriple] = {}
    for k, newtype in prop2range.items():
        if k not in e.props:
            continue
        if newtype in range_constraints:
            if not any(
                stmt.value in range_constraints[newtype]
                for stmt in e.props.get(rdf_type, [])
            ):
                continue
        for val in e.props[k]:
            if not isinstance(val.value, URIRef):
                continue
            out[newtype] = NewTriple(
                str(val.value), rdf_type, newtype, type2contradictions[newtype]
            )
    return list(out.values())


def merge_new_triple(
    tup: tuple[str, tuple[Entity, Optional[Iterable[NewTriple]]]]
) -> Entity:
    id, (ent, triples) = tup

    if triples is not None:
        for triple in triples:
            if triple.predicate not in ent.props:
                ent.props[triple.predicate] = []
            if triple.constraints.isdisjoint(
                (stmt.value for stmt in ent.props[triple.predicate])
            ):
                if any(
                    triple.object == stmt.value for stmt in ent.props[triple.predicate]
                ):
                    continue
                ent.props[triple.predicate].append(
                    Statement(value=triple.object, qualifiers={}, qualifiers_order=[])
                )
    return ent
