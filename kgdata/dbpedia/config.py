"""Locations of DBpedia dumps and datasets on disk."""

import re
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Union

from loguru import logger


class DBpediaDirCfg:
    """Locations of DBpedia dumps and datasets on disk."""

    instance = None

    @staticmethod
    def get_instance():
        if DBpediaDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return DBpediaDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path], verbose: bool = True):
        """Initialize or update the config object to use the given directory"""
        if verbose:
            logger.info("DBpedia directory: {}", datadir)
        DBpediaDirCfg.instance = DBpediaDirCfg(Path(datadir))
        return DBpediaDirCfg.instance

    def __init__(self, datadir: Path):
        self.datadir = datadir

        self.dumps = datadir / "dumps"
        self.ontology_dump = datadir / "ontology_dump"
        self.modifications = datadir / "modifications"
        self.mapping_extractor_dump = datadir / "mapping_extractor_dump"
        self.generic_extractor_dump = datadir / "generic_extractor_dump"

        self.classes = datadir / "classes"
        self.class_count = datadir / "class_count"

        self.properties = datadir / "properties"
        self.entities = datadir / "entities"
        self.entity_types = datadir / "entity_types"
        self.entity_labels = datadir / "entity_labels"
        self.entity_metadata = datadir / "entity_metadata"
        self.entity_all_types = datadir / "entity_all_types"
        self.entity_degrees = datadir / "entity_degrees"
        self.entity_types_and_degrees = datadir / "entity_types_and_degrees"
        self.entity_redirections = datadir / "entity_redirections"

        # mapping from dbpedia resource to wikipedia id
        self.page_id_dump = datadir / "page_id_dump"
        self.wikilink_dump = datadir / "wikilink_dump"

        self.wikilinks = datadir / "wikilinks"

        self.meta_graph = datadir / "meta_graph"
        self.meta_graph_stats = datadir / "meta_graph_stats"

    @lru_cache
    def get_dump_date(self):
        res = re.findall(r"\d{8}", str(self.datadir))
        assert len(res) == 1
        return res[0]

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

    def get_redirection_modified_file(self):
        return self.modifications / "redirections.csv"

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
