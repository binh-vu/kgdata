from __future__ import annotations
import copy
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
from table_extractor.models.html_table import HTMLTable, ContentHierarchy, HTMLTableRow


@dataclass
class LinkedHTMLTable(HTMLTable):
    # mapping from (row, col) => links)
    links: Dict[Tuple[int, int], List[WikiLink]]

    def to_dict(self):
        return {
            "_v": 1,
            "id": self.id,
            "page_url": self.page_url,
            "caption": self.caption,
            "attrs": self.attrs,
            "context": [c.to_dict() for c in self.context],
            "rows": [r.to_dict() for r in self.rows],
            "links": [
                [ri, ci, [l.to_dict() for l in links]]
                for (ri, ci), links in self.links.items()
            ],
        }

    @staticmethod
    def from_dict(o: dict):
        assert o.pop("_v") == 1
        o["context"] = [ContentHierarchy.from_dict(c) for c in o["context"]]
        o["rows"] = [HTMLTableRow.from_dict(r) for r in o["rows"]]
        o["links"] = {
            (ri, ci): [WikiLink.from_dict(l) for l in links]
            for ri, ci, links in o["links"]
        }
        return LinkedHTMLTable(**o)


@dataclass
class WikiLink:
    start: int
    end: int  # exclusive
    wikipedia_url: str
    wikidata_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "start": self.start,
            "end": self.end,
            "wikipedia_url": self.wikipedia_url,
            "wikidata_id": self.wikidata_id,
        }

    @staticmethod
    def from_dict(o: dict):
        return WikiLink(**o)
