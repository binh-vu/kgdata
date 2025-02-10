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
