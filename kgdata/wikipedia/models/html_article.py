from __future__ import annotations
from dataclasses import dataclass
from typing import List, Literal, Optional


@dataclass
class HTMLArticle:
    """Model of the HTML page article from Wikipedia HTML Dumps."""

    # page title, help get access the article by replacing space with underscore
    name: str

    # page id, can help access the article by /?curid=id
    page_id: int

    # utc string specified the modification time of the article
    date_modified: str

    # url of the article
    url: str

    # language of the page e.g., en
    lang: str

    # wikidata entity associated with the page
    wdentity: Optional[str]
    # additional entities associated with the page
    additional_entities: List[AdditionalEntity]

    # part of which wikipedia, e.g., enwiki
    is_part_of: str

    # list of wikipedia categories
    categories: List[NameAndURL]
    # list of wikipedia templates
    templates: List[NameAndURL]

    # list of wikipedia pages that redirect to this page
    redirects: List[NameAndURL]

    # the parsed html
    html: str

    # the wikitext
    wikitext: str

    @staticmethod
    def from_dump_dict(o: dict) -> HTMLArticle:
        additional_entities = []
        for ent in o.get("additional_entities", []):
            assert len(ent) == 3
            additional_entities.append(
                AdditionalEntity(
                    identifier=ent["identifier"], aspects=ent["aspects"], url=ent["url"]
                )
            )

        return HTMLArticle(
            name=o["name"],
            page_id=o["identifier"],
            date_modified=o["date_modified"],
            url=o["url"],
            lang=o["in_language"]["identifier"],
            wdentity=o["main_entity"]["identifier"] if "main_entity" in o else None,
            additional_entities=additional_entities,
            is_part_of=o["is_part_of"]["identifier"],
            categories=[
                NameAndURL(name=x["name"], url=x["url"])
                for x in o.get("categories", [])
            ],
            templates=[
                NameAndURL(name=x["name"], url=x["url"]) for x in o.get("templates", [])
            ],
            redirects=[
                NameAndURL(name=x["name"], url=x["url"]) for x in o.get("redirects", [])
            ],
            html=o["article_body"]["html"],
            wikitext=o["article_body"]["wikitext"],
        )


@dataclass
class AdditionalEntity:
    identifier: str
    url: str
    aspects: List[str]


@dataclass
class NameAndURL:
    name: str
    url: str
