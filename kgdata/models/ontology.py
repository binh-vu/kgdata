from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.models.entity import Entity
from kgdata.models.ont_class import OntologyClass
from kgdata.models.ont_property import OntologyProperty
from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, URIRef
from sm.namespaces.prelude import KGName, KnowledgeGraphNamespace
from sm.typing import InternalID


@dataclass
class Ontology:
    kgname: KGName
    kgns: KnowledgeGraphNamespace
    classes: Mapping[InternalID, OntologyClass]
    props: Mapping[InternalID, OntologyProperty]

    def get_class_label(self, id: InternalID) -> str:
        label = str(self.classes[id].label)
        if self.kgns.has_encrypted_name(self.kgns.id_to_uri(id)):
            return f"{label} ({id})"
        return label

    def get_prop_label(self, id: InternalID) -> str:
        label = str(self.props[id].label)
        if self.kgns.has_encrypted_name(self.kgns.id_to_uri(id)):
            return f"{label} ({id})"
        return label

    @classmethod
    def from_ttl(
        cls, kgname: KGName, kgns: KnowledgeGraphNamespace, ttl_file: Path
    ) -> tuple[Ontology, Mapping[str, Entity]]:
        from kgdata.dbpedia.datasets.entities import to_entity

        g = Graph()
        g.parse(ttl_file)

        classes = {}
        props = {}
        ents = {}

        # parse classes
        source2triples = defaultdict(list)
        for s, p, o in g:
            if isinstance(o, BNode):
                continue
            source2triples[s].append((s, p, o))

        resources = [
            aggregated_triples(x)
            for x in source2triples.items()
            if isinstance(x[0], URIRef)
        ]

        rdf_type = str(RDF.type)

        for resource in resources:
            ent = to_entity(resource, "en")
            ent.id = kgns.uri_to_id(ent.id)

            (stmt,) = ent.props[rdf_type]
            if stmt.value in {RDF.Property, OWL.DatatypeProperty, OWL.ObjectProperty}:
                ranges = ent.props.get(str(RDFS.range), [])
                if len(ranges) == 0:
                    datatype = ""
                else:
                    if any(str(r.value).startswith(str(XSD)) for r in ranges):
                        assert len(ranges) == 1
                        datatype = str(ranges[0].value)
                    else:
                        # this is likely to be an object property
                        datatype = str(XSD.anyURI)
                props[ent.id] = OntologyProperty(
                    id=ent.id,
                    label=ent.label,
                    description=ent.description,
                    aliases=ent.aliases,
                    datatype=datatype,
                    parents=[],
                    related_properties=[],
                    equivalent_properties=[],
                    inverse_properties=[],
                    instanceof=[str(RDF.Property)],
                    ancestors={},
                    domains=[
                        kgns.uri_to_id(stmt.value)
                        for stmt in ent.props.get(str(RDFS.domain), [])
                    ],
                    ranges=[
                        kgns.uri_to_id(stmt.value)
                        for stmt in ent.props.get(str(RDFS.range), [])
                    ],
                )
            elif stmt.value in {RDFS.Class, OWL.Class}:
                classes[ent.id] = OntologyClass(
                    id=ent.id,
                    label=ent.label,
                    description=ent.description,
                    aliases=ent.aliases,
                    parents=[],
                    properties=[],
                    different_froms=[],
                    equivalent_classes=[],
                    ancestors={},
                )
            else:
                # TODO: detect uri and convert them into id?
                ents[ent.id] = ent

        return Ontology(kgname, kgns, classes, props), ents
