"""Locations of DBpedia dumps and datasets on disk."""

from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Union


class DBpediaDirCfg:
    """Locations of DBpedia dumps and datasets on disk."""

    instance = None

    @staticmethod
    def get_instance():
        if DBpediaDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return DBpediaDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        DBpediaDirCfg.instance = DBpediaDirCfg(Path(datadir))
        return DBpediaDirCfg.instance

    def __init__(self, datadir: Path):
        self.datadir = datadir

        self.dumps = datadir / "dumps"
        self.ontology_dump = datadir / "ontology_dump"
        self.mapping_extractor_dump = datadir / "mapping_extractor_dump"
        self.generic_extractor_dump = datadir / "generic_extractor_dump"
        self.redirection_dump = datadir / "redirection_dump"

        self.classes = datadir / "classes"
        self.class_count = datadir / "class_count"

        self.properties = datadir / "properties"
        self.entities = datadir / "entities"
        self.entity_types = datadir / "entity_types"
        self.entity_all_types = datadir / "entity_all_types"
        self.entity_degrees = datadir / "entity_degrees"

        # mapping from dbpedia resource to wikipedia id
        self.page_id_dump = datadir / "page_id_dump"
        self.wikilink_dump = datadir / "wikilink_dump"

        self.wikilinks = datadir / "wikilinks"

    def get_ontology_dump_file(self):
        return self._get_file(self.dumps / "ontology*=parsed.nt")

    def get_generic_extractor_dump_files(self, lang: str = "en"):
        return [self._get_file(self.dumps / f"infobox-properties_lang={lang}.ttl.bz2")]

    def get_mapping_extractor_dump_files(self, lang: str = "en"):
        return [
            self._get_file(self.dumps / f"mappingbased-objects_lang={lang}.ttl.bz2"),
            self._get_file(self.dumps / f"mappingbased-literals_lang={lang}.ttl.bz2"),
            self._get_file(
                self.dumps / f"instance-types_inference=specific_lang={lang}.ttl.bz2"
            ),
        ]

    def get_redirection_dump_file(self, lang: str = "en"):
        return self._get_file(self.dumps / f"redirects_lang={lang}.ttl.bz2")

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
