from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import orjson
from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.entity_types import entity_types
from kgdata.wikidata.models.wdentity import WDEntity

# def get_meta_graph_dataset(with_dep: bool = True):
#     cfg = WikidataDirCfg.get_instance()

#     if with_dep:
#         deps = [entities(), entity_types()]
#     else:
#         deps = []

#     return Dataset(
#         cfg.main_property_connections / "*.gz",
#         deserialize=deser_connection,
#         name="property-connections",
#         dependencies=deps,
#     )


# def meta_graph():
#     entity_outlinks()
