from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

import orjson
from kgdata.dataset import Dataset
from kgdata.db import ser_to_dict
from kgdata.dbpedia.datasets.properties import as_multilingual, rdfs_label
from kgdata.misc.funcs import split_tab_2
from kgdata.misc.resource import RDFResource
from kgdata.models.multilingual import MultiLingualString, MultiLingualStringList
from kgdata.spark.common import get_spark_context
from kgdata.splitter import split_a_list
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.triple_truthy_dump import triple_truthy_dump
from kgdata.wikidata.models.wdentity import EntitySiteLinks, SiteLink, WDEntity
from kgdata.wikidata.models.wdstatement import WDStatement
from kgdata.wikidata.models.wdvalue import (
    WDValueEntityId,
    WDValueGlobeCoordinate,
    WDValueMonolingualText,
    WDValueQuantity,
    WDValueString,
    WDValueTime,
)
from kgdata.wikipedia.misc import get_title_from_url
from rdflib import OWL, RDF, RDFS, SKOS, BNode, Literal, URIRef
from sm.misc.funcs import assert_not_null, is_not_null

rdf_type = str(RDF.type)
rdfs_label = str(RDFS.label)
schema_name = "http://schema.org/name"
schema_about = "http://schema.org/about"
schema_article = "http://schema.org/Article"
skos_prefLabel = str(SKOS.prefLabel)
skos_altLabel = str(SKOS.altLabel)
schema_description = "http://schema.org/description"

owl_sameas = str(OWL.sameAs)

wikibase_namespace = "http://wikiba.se/ontology#"
wikibase_namespace_len = len(wikibase_namespace)
wikibase_badget = f"{wikibase_namespace}badge"
wikibase_propertyType = f"{wikibase_namespace}propertyType"

wikibase_item = f"{wikibase_namespace}Item"
wikibase_property = f"{wikibase_namespace}Property"

wikibase_entity_prefix = "http://www.wikidata.org/entity/"
wikibase_entity_prefix_len = len(wikibase_entity_prefix)

wikibase_prop_direct_prefix = "http://www.wikidata.org/prop/direct/"
wikibase_prop_direct_prefix_len = len(wikibase_prop_direct_prefix)

geo_wkt_literal = "http://www.opengis.net/ont/geosparql#wktLiteral"

norm_datatype = {
    "CommonsMedia": "commonsMedia",
    "ExternalId": "external-id",
    "GeoShape": "geo-shape",
    "GlobeCoordinate": "globe-coordinate",
    "Math": "math",
    "Monolingualtext": "monolingualtext",
    "MusicalNotation": "musical-notation",
    "Quantity": "quantity",
    "String": "string",
    "TabularData": "tabular-data",
    "Time": "time",
    "Url": "url",
    "WikibaseForm": "wikibase-form",
    "WikibaseItem": "wikibase-item",
    "WikibaseLexeme": "wikibase-lexeme",
    "WikibaseProperty": "wikibase-property",
    "WikibaseSense": "wikibase-sense",
}


@dataclass
class TruthyDumpDerivativeDatasets:
    entities: Dataset[WDEntity]
    redirections: Dataset[tuple[str, str]]
    sitelinks: Dataset[EntitySiteLinks]


@lru_cache()
def triple_truthy_dump_derivatives(lang: str = "en") -> TruthyDumpDerivativeDatasets:
    cfg = WikidataDirCfg.get_instance()

    prop_ds = Dataset(
        cfg.truthy_dump_derivatives / "props/*.gz",
        name="entities_from_truthy_dump/props",
        deserialize=RDFResource.deser,
        dependencies=[triple_truthy_dump()],
    )

    sitelink_ds: Dataset[EntitySiteLinks] = Dataset(
        cfg.truthy_dump_derivatives / "sitelinks/*.gz",
        name="triple_truthy_dump_derivatives/sitelinks",
        deserialize=orjson.loads,
        dependencies=[triple_truthy_dump()],
    )

    ent_ds = Dataset(
        cfg.truthy_dump_derivatives / "entities/*.gz",
        name="entities_from_truthy_dump/entities",
        deserialize=deser_entity,
        dependencies=[triple_truthy_dump()],
    )

    raw_redirection_ds = Dataset(
        cfg.truthy_dump_derivatives / "redirections-raw/*.gz",
        name="triple_truthy_dump_derivatives/redirections",
        deserialize=split_tab_2,
        dependencies=[triple_truthy_dump()],
    )
    final_redirection_ds = Dataset(
        cfg.truthy_dump_derivatives / "redirections/*.gz",
        name="triple_truthy_dump_derivatives/redirections",
        deserialize=split_tab_2,
        dependencies=[triple_truthy_dump()],
    )

    if not prop_ds.has_complete_data():
        wikibase_property = URIRef("http://wikiba.se/ontology#Property")
        (
            triple_truthy_dump()
            .get_extended_rdd()
            .filter(lambda res: wikibase_property in res.props.get(rdf_type, []))
            .map(RDFResource.ser)
            .save_like_dataset(prop_ds, auto_coalesce=True, shuffle=True)
        )

    if not sitelink_ds.has_complete_data():
        (
            triple_truthy_dump()
            .get_extended_rdd()
            .map(get_site_links)
            .group_by_key_skip_null()
            .map(
                lambda id_sites: EntitySiteLinks(
                    id=id_sites[0],
                    sitelinks={site.site: site for site in id_sites[1]},
                )
            )
            .map(ser_to_dict)
            .save_like_dataset(sitelink_ds, auto_coalesce=True, shuffle=True)
        )
        assert sitelink_ds.get_extended_rdd().count() == 0

    if not ent_ds.has_complete_data():
        p2type = get_spark_context().broadcast(get_property_to_type(prop_ds))

        (
            triple_truthy_dump()
            .get_extended_rdd()
            .map(lambda x: to_entity(x, lang, p2type.value))
            .filter(lambda x: x is not None)
            .map(lambda x: ser_entity(assert_not_null(x)))
            .save_like_dataset(ent_ds, auto_coalesce=True, shuffle=True)
        )

    # upd_ent_ds = Dataset(
    #     cfg.truthy_dump_derivatives / "entities-upd/*.gz",
    #     name="triple_truthy_dump_derivatives/entities",
    #     deserialize=deser_entity,
    #     dependencies=[triple_truthy_dump()],
    # )
    # if not upd_ent_ds.has_complete_data():
    #     def add_sitelink(
    #         id: str, ent: WDEntity, sitelinks: Optional[dict[str, SiteLink]]
    #     ):
    #         if sitelinks is not None:
    #             ent.sitelinks = sitelinks
    #         return ent

    #     (
    #         ent_ds.get_extended_rdd()
    #         .map(lambda x: (x.id, x))
    #         .leftOuterJoin(
    #             sitelink_ds.get_extended_rdd().map(lambda x: (x["id"], x["sitelinks"]))
    #         )
    #         .map(lambda x: add_sitelink(x[0], x[1][0], x[1][1]))
    #         .map(ser_entity)
    #         .save_like_dataset(
    #             upd_ent_ds,
    #             auto_coalesce=True,
    #             shuffle=True,
    #             trust_dataset_dependencies=True,
    #         )
    #     )

    if not raw_redirection_ds.has_complete_data():
        (
            triple_truthy_dump()
            .get_extended_rdd()
            .map(lambda x: get_redirections(x))
            .filter(lambda x: x is not None)
            .map(lambda x: "\t".join(assert_not_null(x)))
            .save_like_dataset(raw_redirection_ds, auto_coalesce=True, shuffle=True)
        )

    if not final_redirection_ds.has_complete_data():
        map = raw_redirection_ds.get_dict()
        multi_map = [k for k, v in Counter(map.keys()).items() if v > 1]
        if len(multi_map) > 0:
            raise ValueError(f"Multiple redirections for the same entity: {multi_map}")

        # for k, v in map.items():
        #     if v in map:
        #         raise ValueError(f"We found multi-hop redirections")

        newmap = {}
        for k, v in map.items():
            while v in map:
                v = map[v]
            newmap[k] = v

        split_a_list(
            ["\t".join(x).encode() for x in newmap.items()],
            Path(final_redirection_ds.file_pattern).parent / "part.gz",
        )
        final_redirection_ds.sign(
            final_redirection_ds.get_name(), final_redirection_ds.dependencies
        )

    return TruthyDumpDerivativeDatasets(
        entities=ent_ds, redirections=final_redirection_ds, sitelinks=sitelink_ds
    )


def get_property_to_type(prop_ds: Dataset[RDFResource]) -> dict[str, str]:
    # mapping from property id into the data type
    # the list of short, ~11K so we can keep them in memory
    pid2type = {}
    for prop in prop_ds.get_list():
        pid = prop.id[wikibase_entity_prefix_len:]
        (property_type,) = prop.props[wikibase_propertyType]
        assert property_type.startswith(wikibase_namespace)
        pid2type[pid] = norm_datatype[property_type[wikibase_namespace_len:]]
    return pid2type


def get_redirections(resource: RDFResource) -> Optional[tuple[str, str]]:
    if not resource.id.startswith("http://www.wikidata.org/entity/"):
        return None

    if owl_sameas not in resource.props:
        return None

    # this is redirection, only has 1 same as link
    assert len(resource.props) == 1, (resource.id, resource.props)

    (val,) = resource.props[owl_sameas]
    assert isinstance(val, URIRef)
    assert val.startswith(wikibase_entity_prefix)
    return resource.id[wikibase_entity_prefix_len:], val[wikibase_entity_prefix_len:]


def get_entity_id_from_url(url: str):
    assert url.startswith(wikibase_entity_prefix)
    return url[wikibase_entity_prefix_len:]


def get_site_links(resource: RDFResource) -> Optional[tuple[str, SiteLink]]:
    id = resource.id
    if not id.startswith("http"):
        return None

    o = urlparse(id)
    assert ":" not in o.netloc, (o.netloc, "should not have port numbers")
    if not o.netloc.endswith("wikipedia.org") or rdf_type not in resource.props:
        return None

    if not any(str(rtype) == schema_article for rtype in resource.props[rdf_type]):
        return None

    m = re.match(r"([a-z]+)\.wikipedia\.org", o.netloc)
    assert m is not None
    (enturl,) = resource.props[schema_about]
    entid = get_entity_id_from_url(enturl)
    return entid, SiteLink(
        site=m.group(1) + "wiki",
        title=get_title_from_url(id),
        badges=[
            get_entity_id_from_url(badge)
            for badge in resource.props.get(wikibase_badget, [])
        ],
        url=id,
    )


def to_entity(
    resource: RDFResource, default_lang: str, p2type: dict[str, str]
) -> Optional[WDEntity]:
    """Convert Wikidata RDF dump to WDEntity

    Reference: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format

    """
    id = resource.id
    if id.startswith("https://www.wikidata.org/wiki/Special:EntityData/"):
        # skip this Special:EntityData/XXX, there is a real record XXX
        # checkout Entity Representation
        return None

    if id == "http://wikiba.se/ontology#Dump":
        # skip this special id
        return None

    if id.startswith("http://www.wikidata.org/entity/"):
        if owl_sameas in resource.props:
            # this is redirection, only has 1 same as link
            assert len(resource.props) == 1, (resource.id, resource.props)
            return None

        assert rdf_type in resource.props, resource.id
        if any(str(raw_type) == wikibase_item for raw_type in resource.props[rdf_type]):
            assert not any(
                str(raw_type) == wikibase_property
                for raw_type in resource.props[rdf_type]
            )
            type = "item"
        elif any(
            str(raw_type) == wikibase_property for raw_type in resource.props[rdf_type]
        ):
            type = "property"
        else:
            raise ValueError(f"Unknown type: {resource.props[rdf_type]} for {id}")

        datatype = None
        if wikibase_propertyType in resource.props:
            (datatype,) = resource.props[wikibase_propertyType]
            datatype = datatype[wikibase_namespace_len:]
            datatype = norm_datatype[datatype]

        props = {}
        for prop, vals in resource.props.items():
            if prop.startswith(wikibase_prop_direct_prefix):
                pid = prop[wikibase_prop_direct_prefix_len:]
                if pid not in p2type:
                    continue
                pvals = [
                    to_statement(id, val, p2type[pid])
                    for val in vals
                    if not isinstance(val, BNode)
                ]
                if len(pvals) > 0:
                    props[pid] = pvals
        ent = WDEntity(
            id=id[wikibase_entity_prefix_len:],
            type=type,
            label=extract_entity_label(resource, default_lang),
            aliases=extract_entity_aliases(resource, default_lang),
            description=extract_entity_description(resource, default_lang),
            datatype=datatype,
            props=props,
            sitelinks={},
        )
        return ent

    if not id.startswith("http"):
        # verify that bnode doesn't contain information that we want so we can ignore them
        ignore_props = {
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "http://www.w3.org/2002/07/owl#onProperty",
            "http://www.w3.org/2002/07/owl#someValuesFrom",
        }
        assert all(prop in ignore_props for prop in resource.props)
    return None
    # redirection is implemented via sameAs


def to_statement(
    id: str, value: URIRef | BNode | Literal, valuetype: str
) -> WDStatement:
    assert not isinstance(value, BNode), (id, value, valuetype)

    if isinstance(value, URIRef):
        if valuetype in [
            "wikibase-item",
            "wikibase-property",
            "wikibase-lexeme",
            "wikibase-form",
            "wikibase-sense",
        ]:
            assert value.startswith(wikibase_entity_prefix), (value, valuetype)
            if valuetype not in ["wikibase-form", "wikibase-sense"]:
                assert value[wikibase_entity_prefix_len + 1 :].isdigit()
                numeric_id = int(value[wikibase_entity_prefix_len + 1 :])
            else:
                numeric_id = -1

            if valuetype == "wikibase-item":
                assert value[wikibase_entity_prefix_len] == "Q"
                entity_type = "item"
            elif valuetype == "wikibase-property":
                assert value[wikibase_entity_prefix_len] == "P"
                entity_type = "property"
            elif valuetype == "wikibase-lexeme":
                assert value[wikibase_entity_prefix_len] == "L"
                entity_type = "lexeme"
            elif valuetype == "wikibase-form":
                assert value[wikibase_entity_prefix_len] == "L"
                entity_type = "form"
            elif valuetype == "wikibase-sense":
                assert value[wikibase_entity_prefix_len] == "L"
                entity_type = "sense"
            else:
                raise ValueError(f"Unknown entity type: {value} {valuetype}")

            wdvalue = WDValueEntityId(
                "wikibase-entityid",
                {
                    "entity-type": entity_type,  # type: ignore
                    "id": value[wikibase_entity_prefix_len:],
                    "numeric-id": numeric_id,
                },
            )
        elif valuetype in ["commonsMedia", "url", "geo-shape", "tabular-data"]:
            wdvalue = WDValueString("string", str(value))
        else:
            raise NotImplementedError((value, valuetype))
    else:
        assert isinstance(value, Literal), (value, valuetype)
        if valuetype in ["string", "external-id", "math", "musical-notation"]:
            wdvalue = WDValueString("string", value)
        elif valuetype == "monolingualtext":
            wdvalue = WDValueMonolingualText(
                "monolingualtext",
                {"text": value.value, "language": assert_not_null(value.language)},
            )
        elif valuetype == "globe-coordinate":
            assert str(value.datatype) == geo_wkt_literal, (value, valuetype)
            value_str = str(value)
            if value_str.startswith("<http://www.wikidata.org/entity/Q"):
                # they have different value for the globe
                globe, value_str = value_str.split(">", 1)
                value_str = value_str.lstrip()
                globe = globe[1:]
                assert globe.startswith(wikibase_entity_prefix)
            else:
                globe = "http://www.wikidata.org/entity/Q2"

            m = re.match(
                r"Point\((?P<long>-?[0-9\.E-]+) (?P<lat>-?[0-9\.E-]+)\)", value_str
            )
            assert m is not None, (value, valuetype)
            long = float(m.group("long"))
            lat = float(m.group("lat"))
            # using value as recommended in the document: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Globe_coordinate
            wdvalue = WDValueGlobeCoordinate(
                "globecoordinate",
                {
                    "latitude": lat,
                    "longitude": long,
                    "precision": 0.000277778,
                    "altitude": None,
                    "globe": globe,
                },
            )
        elif valuetype == "quantity":
            amount = str(value)
            wdvalue = WDValueQuantity(
                "quantity",
                {
                    "amount": amount,
                    "upperBound": amount,
                    "lowerBound": amount,
                    "unit": "http://www.wikidata.org/entity/Q199",
                },
            )
        elif valuetype == "time":
            wdvalue = WDValueTime(
                "time",
                {
                    "time": str(value),
                    "precision": 11,  # days -- we can't know the precision from the dump
                    "before": 0,
                    "after": 0,
                    "timezone": 0,
                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                },
            )
        else:
            raise NotImplementedError((value, valuetype))

    return WDStatement(value=wdvalue, qualifiers={}, qualifiers_order=[], rank="normal")


def extract_entity_label(
    resource: RDFResource,
    default_lang: str = "en",
    reduce_size: bool = True,
):
    """Extract labels of an entity. Wikidata triple truthy dump provides
    four properties that can contain labels:
    1. rdfs:label
    2. schema:name
    3. skos:altLabel
    4. skos:prefLabel

    (3) skos:altLabel provides alternative labels for an entity.
    (1) directly provides the label of the entity:
        - (2) is mostly duplicated of (1) -- verified in the dump of 2024-03-20 that only
            74 records are different between (1) and (2)

    In the RDF dump document, they mention:
    "For labels, only rdfs:label is stored but not schema:name or skos:prefLabel. Since they all have the same data, storing all three is redundant."

    Therefore, we should just take union of the three.
    """
    if rdfs_label not in resource.props:
        assert (
            skos_prefLabel not in resource.props and schema_name not in resource.props
        )
        return MultiLingualString({default_lang: ""}, lang=default_lang)

    lang2label = {}
    for key in [rdfs_label, schema_name, skos_prefLabel]:
        for label in resource.props[key]:
            assert isinstance(label, Literal)
            if label.language in lang2label:
                assert lang2label[label.language] == label.value
            else:
                lang2label[label.language] = label.value

    if None in lang2label:
        if default_lang in lang2label:
            assert lang2label[None] == lang2label[default_lang]
        else:
            lang2label[default_lang] = lang2label.pop(None)

    if default_lang not in lang2label:
        lang2label[default_lang] = ""

    if reduce_size:
        # remove duplicated labels in different languages to reduce the size of the output
        # knowing this will lose information
        val = lang2label[default_lang]
        for other_lang in list(lang2label):
            if other_lang != default_lang and lang2label[other_lang] == val:
                del lang2label[other_lang]

    return as_multilingual(
        [Literal(label, lang=lang) for lang, label in lang2label.items()],
        default_lang=default_lang,
    )


def extract_entity_aliases(
    resource: RDFResource,
    default_lang: str = "en",
):
    if skos_altLabel not in resource.props:
        return MultiLingualStringList({default_lang: []}, lang=default_lang)

    lang2aliases = {}
    for val in resource.props.get(skos_altLabel, []):
        assert isinstance(val, Literal)
        lang = val.language or default_lang
        if lang in lang2aliases:
            lang2aliases[lang].append(val.value)
        else:
            lang2aliases[lang] = [val.value]

    if default_lang not in lang2aliases:
        lang2aliases[default_lang] = []
    return MultiLingualStringList(lang2aliases, lang=default_lang)


def extract_entity_description(
    resource: RDFResource,
    default_lang: str = "en",
):
    if schema_name not in resource.props:
        return MultiLingualString({default_lang: ""}, lang=default_lang)

    lang2desc = {}
    for val in resource.props.get(schema_description, []):
        assert isinstance(val, Literal)
        lang = val.language or default_lang
        if lang in lang2desc:
            assert lang2desc[lang] == val.value
        else:
            lang2desc[lang] = val.value

    if default_lang not in lang2desc:
        lang2desc[default_lang] = ""
    return MultiLingualString(lang2desc, lang=default_lang)


def ser_entity(ent: WDEntity):
    return orjson.dumps(ent.to_dict())


def deser_entity(line: Union[str, bytes]) -> WDEntity:
    return WDEntity.from_dict(orjson.loads(line))
