from dataclasses import dataclass
from pathlib import Path
from typing import Union
from glob import glob


class WDDataDirCfg:
    """Locations of Wikidata dumps and datasets on disk"""

    instance = None

    def __init__(self, datadir: Path) -> None:
        self.datadir = datadir

        # directorys contain dumps and their splitted files
        # for the name of the dumps, see the corresponding function `self.get_X_file` in this class
        self.dumps = datadir / "dumps"
        self.entity_dump = datadir / "entity_dump"
        self.page_dump = datadir / "page_dump"
        self.entity_redirection_dump = datadir / "entity_redirection_dump"

        self.entity_ids = datadir / "entity_ids"
        self.page_ids = datadir / "page_ids"
        self.entities = datadir / "entities"

        self.classes = datadir / "classes"
        self.properties = datadir / "properties"
        self.property_domains = datadir / "property_domains"
        self.property_ranges = datadir / "property_ranges"
        self.wp2wd = datadir / "wp2wd"

        self.entity_redirections = datadir / "entity_redirections"

    def get_entity_dump_file(self):
        return self._get_file(self.dumps / "*wikidata-*all*.json.bz2")

    def get_page_dump_file(self):
        return self._get_file(self.dumps / "*wikidatawiki-*page*.sql.gz")

    def get_redirect_dump_file(self):
        return self._get_file(self.dumps / "*wikidatawiki-*redirect*.sql.gz")

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
        if WDDataDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return WDDataDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        WDDataDirCfg.instance = WDDataDirCfg(Path(datadir))
        return WDDataDirCfg.instance
