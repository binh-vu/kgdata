"""Locations of DBpedia dumps and datasets on disk."""

from dataclasses import dataclass
from pathlib import Path
from typing import Union
from glob import glob


class DBpediaDataDirCfg:
    """Locations of DBpedia dumps and datasets on disk."""

    instance = None

    @staticmethod
    def get_instance():
        if DBpediaDataDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return DBpediaDataDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        DBpediaDataDirCfg.instance = DBpediaDataDirCfg(Path(datadir))
        return DBpediaDataDirCfg.instance

    def __init__(self, datadir: Path):
        self.datadir = datadir

        self.dumps = datadir / "dumps"
        self.infobox_property_dump = datadir / "infobox_property_dump"
        # mapping from dbpedia resource to wikipedia id
        self.page_id_dump = datadir / "page_id_dump"
        self.wikilink_dump = datadir / "wikilink_dump"

        self.wikilinks = datadir / "wikilinks"

    def get_infobox_property_dump_file(self, lang: str = "en"):
        return self._get_file(self.dumps / f"infobox-properties_lang={lang}.ttl.bz2")

    def get_wikilink_dump_file(self, lang: str = "en"):
        return self._get_file(self.dumps / f"wikilinks_lang={lang}.ttl.bz2")

    def get_page_id_dump_file(self, lang: str = "en"):
        return self._get_file(self.dumps / f"page_lang={lang}_ids.ttl.bz2")

    def _get_file(self, file: Union[str, Path]):
        file = str(file)
        match_files = glob(file)
        if len(match_files) == 0:
            raise Exception("No file found: {}".format(file))
        if len(match_files) > 1:
            raise Exception("Multiple files found: {}".format(file))
        return Path(match_files[0])
