from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from kgdata.models.ont_class import OntologyClass
from kgdata.models.ont_property import OntologyProperty
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
