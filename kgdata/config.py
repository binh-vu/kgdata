import os
from pathlib import Path

from loguru import logger

from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikipedia.config import WikipediaDirCfg

DEFAULT_DATA_DIR = Path(os.path.abspath(__file__)).parent.parent.parent / "data"

DBPEDIA_DIR = os.environ.get("DBPEDIA_DIR", str(DEFAULT_DATA_DIR / "dbpedia"))


def init_dbdir_from_env():
    DBP_DIR_NAME = "DBP_DIR"
    WD_DIR_NAME = "WD_DIR"
    WP_DIR_NAME = "WP_DIR"

    log_config = os.environ.get("LOG_CONFIG", "1")

    if WD_DIR_NAME not in os.environ:
        raise KeyError(f"Need the env variable {WD_DIR_NAME} to set Wikidata directory")

    if log_config == "1":
        logger.info("Wikidata directory: {}", os.environ[WD_DIR_NAME])
    WikidataDirCfg.init(os.environ[WD_DIR_NAME])

    if DBP_DIR_NAME not in os.environ:
        raise KeyError(f"Need the env variable {DBP_DIR_NAME} to set DBpedia directory")

    if log_config == "1":
        logger.info("DBpedia directory: {}", os.environ[DBP_DIR_NAME])
    DBpediaDirCfg.init(os.environ[DBP_DIR_NAME])

    if WP_DIR_NAME not in os.environ:
        raise KeyError(
            f"Need the env variable {WP_DIR_NAME} to set Wikipedia directory"
        )

    if log_config == "1":
        logger.info("Wikipedia directory: {}", os.environ[WP_DIR_NAME])
    WikipediaDirCfg.init(os.environ[WP_DIR_NAME])


if __name__ == "__main__":
    init_dbdir_from_env()