from __future__ import annotations

from functools import partial
from typing import Optional

import orjson
from rdflib import BNode, Literal, URIRef

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.generic_extractor_dump import generic_extractor_dump
from kgdata.dbpedia.datasets.mapping_extractor_dump import mapping_extractor_dump
from kgdata.dbpedia.datasets.properties import (
    as_multilingual,
    assert_all_literal,
    rdfs_comment,
    rdfs_label,
)
from kgdata.misc.resource import RDFResource
from kgdata.models.entity import Entity, Statement
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.misc import get_title_from_url


def entities(lang: str = "en") -> Dataset[Entity]:
    cfg = DBpediaDirCfg.get_instance()

    outdir = cfg.entities.parent / f"{cfg.entities.name}_{lang}"

    if not does_result_dir_exist(outdir):
        part1 = mapping_extractor_dump(lang).get_rdd().map(lambda r: (r.id, r))
        part2 = generic_extractor_dump(lang).get_rdd().map(lambda r: (r.id, r))

        (
            part1.fullOuterJoin(part2)
            .map(lambda x: merge_resources(x[1][0], x[1][1]))
            .map(partial(to_entity, dump_lang=lang))
            .map(lambda c: orjson.dumps(c.to_dict()))
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        outdir / "*.gz",
        deserialize=lambda x: Entity.from_dict(orjson.loads(x)),
    )


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


def to_entity(resource: RDFResource, dump_lang: str):
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
