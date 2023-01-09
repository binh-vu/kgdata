"""
Run the command ``python -m kgdata.wikidata.datasets`` to generate the datasets.

.. code:: bash

   $ python -m kgdata.wikidata.datasets --help
   
    Usage: python -m kgdata.wikidata.datasets [OPTIONS]

    Options:
    -s, --source TEXT   Wikidata directory  [required]
    -d, --dataset TEXT  Dataset name  [required]
    --dbpedia TEXT      DBpedia directory. Only needed if building datasets that
                        require DBpedia data such as entity_wikilinks
    --help              Show this message and exit.

Examples::

    python -m kgdata.wikidata.datasets -s $WD_DIR -d entities
    python -m kgdata.wikidata.datasets -s $WD_DIR -d classes
    python -m kgdata.wikidata.datasets -s $WD_DIR -d properties

.. note::

    For the commands in the above examples to run correctly, replaced ``$WD_DIR`` with the path to the Wikidata directory, e.g., ``export WD_DIR=/data/wikidata/20211213``

"""

from importlib import import_module
from typing import Optional
from typing_extensions import Required
import click
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.dbpedia.config import DBpediaDataDirCfg
import kgdata.wikidata.datasets
from loguru import logger


@click.command("Generate a specific dataset")
@click.option("-s", "--source", required=True, help="Wikidata directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
@click.option(
    "--dbpedia",
    required=False,
    help="DBpedia directory. Only needed if building datasets that require DBpedia data such as entity_wikilinks",
)
def main(source: str, dataset: str, dbpedia: Optional[str] = None):
    logger.info("Wikidata directory: {}", source)

    WDDataDirCfg.init(source)
    if dbpedia is not None:
        DBpediaDataDirCfg.init(dbpedia)

    module = import_module(f"kgdata.wikidata.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
