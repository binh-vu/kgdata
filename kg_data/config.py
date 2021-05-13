import os
from pathlib import Path

DEFAULT_DATA_DIR = Path(os.path.abspath(__file__)).parent.parent.parent.parent.parent / "data"

DBPEDIA_DIR = os.environ.get('DBPEDIA_DIR', str(DEFAULT_DATA_DIR / "dbpedia"))
WIKIDATA_DIR = os.environ.get('WIKIDATA_DIR', str(DEFAULT_DATA_DIR / "wikidata"))
WIKIPEDIA_DIR = os.environ.get('WIKIPEDIA_DIR', str(DEFAULT_DATA_DIR / "wikipedia"))
WIKIDATA_REDIS = os.environ.get("WIKIDATA_REDIS", 'redis://:smdeV1$@localhost:26379/0')
DBPEDIA_REDIS = os.environ.get("DBPEDIA_REDIS", 'redis://:smdeV1$@localhost:26379/1')
WIKIDATA_ONTOLOGY_REDIS = os.environ.get("WIKIDATA_ONTOLOGY_REDIS", 'redis://:smdeV1$@localhost:26379/2')
