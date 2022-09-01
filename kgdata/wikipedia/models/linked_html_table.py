from __future__ import annotations
import copy, orjson
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple, Union
from rsoup.rsoup import Table


@dataclass
class LinkedHTMLTable:
    table: Table
    # mapping from (row, col) => links)
    links: Dict[Tuple[int, int], List[WikiLink]]

    def to_json(self) -> bytes:
        links = [
            [ri, ci, [l.to_dict() for l in links]]
            for (ri, ci), links in self.links.items()
        ]
        return orjson.dumps([self.table.to_base64(), links])

    @staticmethod
    def from_json(s: Union[str, bytes]) -> LinkedHTMLTable:
        tbl, links = orjson.loads(s)
        links = {
            (ri, ci): [WikiLink.from_dict(l) for l in links] for ri, ci, links in links
        }
        return LinkedHTMLTable(
            table=Table.from_base64(tbl),
            links=links,
        )


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
