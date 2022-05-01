from typing import Optional
from dataclasses import dataclass

from kgdata.wikipedia.config import WikipediaConfig


@dataclass
class WikiPageArticle:
    # Original schema: https://www.mediawiki.org/xml/export-0.10.xsd
    # properties of PageType
    # page.id
    id: int
    # page.ns
    ns: str
    # page.title
    title: str
    # page.redirect.title (since RedirectType only has title property), flag this current revision is a redirect
    redirect_title: Optional[str]

    # properties of revision
    # page.revision.model
    model: str
    # page.revision.format
    format: str
    # page.revision.text
    text: str

    @property
    def url(self):
        return f"{WikipediaConfig.WikiURL}/wiki/{self.title.replace(' ', '_')}?curid={self.id}"
