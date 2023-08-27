import re
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Union


class WikipediaConfig:
    # url of the wikipedia server, not trailing slash
    WikiURL = "https://en.wikipedia.org"


class WikipediaDirCfg:
    instance = None

    def __init__(self, datadir: Path) -> None:
        self.datadir = datadir

        # directorys contain dumps and their splitted files
        # for the name of the dumps, see the corresponding function `self.get_X_file` in this class
        self.dumps = datadir / "dumps"
        self.articles = datadir / "articles"
        self.grouped_articles = datadir / "grouped_articles"
        self.html_articles = datadir / "html_articles"
        self.html_tables = datadir / "html_tables"
        self.article_metadata = datadir / "article_metadata"
        self.article_links = datadir / "article_links"
        self.article_aliases = datadir / "article_aliases"
        self.article_degrees = datadir / "article_degrees"
        self.relational_tables = datadir / "relational_tables"
        self.linked_relational_tables = datadir / "linked_relational_tables"
        self.easy_tables = datadir / "easy_tables"
        self.easy_tables_metadata = datadir / "easy_tables_metadata"

    @lru_cache
    def get_dump_date(self):
        res = re.findall(r"\d{8}", str(self.datadir))
        assert len(res) == 1
        return res[0]

    def get_article_file(self):
        return self._get_file(self.dumps / "*pages-articles*.xml.bz2")

    def get_html_article_file(self):
        return self._get_file(self.dumps / "*NS0-*ENTERPRISE-HTML.json.tar.gz")

    def _get_file(self, file: Union[str, Path]):
        file = str(file)
        match_files = glob(file)
        if len(match_files) == 0:
            raise Exception("No file found: {}".format(file))
        if len(match_files) > 1:
            raise Exception("Multiple files found: {}".format(file))
        return Path(match_files[0])

    @staticmethod
    def get_instance():
        if WikipediaDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return WikipediaDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        WikipediaDirCfg.instance = WikipediaDirCfg(Path(datadir))
        return WikipediaDirCfg.instance
