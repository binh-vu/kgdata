"""Locations of Wikidata dumps and datasets on disk."""

import re
from functools import lru_cache
from glob import glob
from pathlib import Path
from typing import Union


class WikidataDirCfg:
    """Locations of Wikidata dumps and datasets on disk"""

    instance = None

    def __init__(self, datadir: Path) -> None:
        self.datadir = datadir

        # directorys contain dumps and their splitted files
        # for the name of the dumps, see the corresponding function `self.get_X_file` in this class
        self.dumps = datadir / "000_dumps"
        self.modification = datadir / "001_modifications"
        self.entity_dump = datadir / "012_entity_dump"
        self.page_dump = datadir / "010_page_dump"
        self.entity_redirection_dump = datadir / "011_entity_redirection_dump"
        self.triple_truthy_dump = datadir / "013_triple_truthy_dump"

        self.page_ids = datadir / "020_page_ids"
        self.entity_ids = datadir / "021_entity_ids"
        self.entity_redirections = datadir / "022_entity_redirections"
        self.entities = datadir / "023_entities"
        self.entity_types = datadir / "024_entity_types"

        self.classes = datadir / "040_classes"
        self.properties = datadir / "041_properties"
        self.class_count = datadir / "042_class_count"
        self.property_count = datadir / "043_property_count"
        self.property_domains = datadir / "044_property_domains"
        self.property_ranges = datadir / "045_property_ranges"
        self.property_ranges = datadir / "045_property_ranges"
        self.ont_count = datadir / "046_ont_count"
        self.main_property_connections = datadir / "047_main_property_connections"

        self.cross_wiki_mapping = datadir / "050_cross_wiki_mapping"

        self.entity_metadata = datadir / "070_entity_metadata"
        self.entity_labels = datadir / "071_entity_labels"
        self.entity_all_types = datadir / "072_entity_all_types"
        self.entity_outlinks = datadir / "073_entity_outlinks"
        self.entity_pagerank = datadir / "074_entity_pagerank"
        self.entity_degrees = datadir / "075_entity_degrees"
        self.entity_types_and_degrees = datadir / "076_entity_types_and_degrees"
        self.entity_wiki_aliases = datadir / "077_entity_wiki_aliases"

        self.meta_graph = datadir / "080_meta_graph"
        self.meta_graph_stats = datadir / "081_meta_graph_stats"

        self.mention_to_entities = datadir / "090_mention_to_entities"
        self.norm_mentions = datadir / "091_norm_mentions"

        # deprecated
        self.wp2wd = datadir / "wp2wd"
        self.search = datadir / "search"

    @lru_cache
    def get_dump_date(self):
        res = re.findall(r"\d{8}", str(self.datadir))
        assert len(res) == 1
        return res[0]

    def get_entity_dump_file(self):
        try:
            return self._get_file(self.dumps / "*wikidata-*all*.json.zst")
        except:
            return self._get_file(self.dumps / "*wikidata-*all*.json.bz2")

    def get_triple_truthy_dump_file(self):
        return self._get_file(self.dumps / "*wikidata-*truthy*.nt.*")

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
        if WikidataDirCfg.instance is None:
            raise Exception("The config object must be initialized before use")
        return WikidataDirCfg.instance

    @staticmethod
    def init(datadir: Union[str, Path]):
        """Initialize or update the config object to use the given directory"""
        WikidataDirCfg.instance = WikidataDirCfg(Path(datadir))
        return WikidataDirCfg.instance
