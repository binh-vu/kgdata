from __future__ import annotations

import os
from pathlib import Path
from socket import gethostname

from dotenv import dotenv_values, load_dotenv
from kgdata.wikidata.config import WikidataDirCfg

"""Helper to break cycles in the Wikidata ontology

For automatic cycle breaking, we first construct a directed graph of the classes, run cycle detections.

For cycles of two edges, we query wikidata for the latest definition. Then remove the link that is not in the latest revision.
"""


def setup_env():
    load_dotenv(Path(__file__).parent / "env" / f"{gethostname()}.env")

    WD_DIR = Path(os.environ["WD_DIR"])
    WD_DBDIR = Path(os.environ["WD_DBDIR"])

    DATA_DIR = Path(os.environ["DATA_DIR"])

    WikidataDirCfg.init(WD_DIR)
