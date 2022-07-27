from __future__ import annotations
import copy
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
from table_extractor.models.html_table import HTMLTable


@dataclass
class LinkedHTMLTable(HTMLTable):
    # mapping from (row => (col => links))
    links: Dict[str, Dict[str, List[WikiLink]]]


@dataclass
class WikiLink:
    start: int
    end: int  # exclusive
    wikipedia_url: str
    wikidata_id: Optional[str] = None
