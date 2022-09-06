from dataclasses import dataclass
from pathlib import Path
from typing import Union
from glob import glob


class WikipediaConfig:
    # url of the wikipedia server, not trailing slash
    WikiURL = "https://en.wikipedia.org"


class WPDataDirConfig:
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
        self.relational_tables = datadir / "relational_tables"
        self.linked_relational_tables = datadir / "linked_relational_tables"
        self.easy_tables = datadir / "easy_tables"

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
        if WPDataDirConfig.instance is None:
            raise Exception("The config object must be initialized before use")
        return WPDataDirConfig.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        WPDataDirConfig.instance = WPDataDirConfig(Path(datadir))
        return WPDataDirConfig.instance
