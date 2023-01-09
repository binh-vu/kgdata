"""
Run the command ``python -m kgdata.dbpedia.datasets`` to generate the datasets.

.. code:: bash

   $ python -m kgdata.dbpedia.datasets --help
   
   Usage: python -m kgdata.dbpedia.datasets [OPTIONS]

   Options:
   -s, --source TEXT   DBpedia directory  [required]
   -d, --dataset TEXT  Dataset name  [required]
   --help              Show this message and exit. 

Examples::

    python -m kgdata.dbpedia.datasets -s $DBPEDIA_DIR -d entities
    python -m kgdata.dbpedia.datasets -s $DBPEDIA_DIR -d classes
    python -m kgdata.dbpedia.datasets -s $DBPEDIA_DIR -d properties

.. note::

    For the commands in the above examples to run correctly, replaced ``$DBPEDIA_DIR`` with the path to the DBpedia directory, e.g., ``export DBPEDIA_DIR=/data/dbpedia/20211213``

"""

from importlib import import_module
from typing_extensions import Required
import click
from kgdata.dbpedia.config import DBpediaDataDirCfg
import kgdata.dbpedia.datasets
from loguru import logger


@click.command("Generate a specific dataset")
@click.option("-s", "--source", required=True, help="DBpedia directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
def main(source: str, dataset: str):
    logger.info("DBpedia directory: {}", source)

    DBpediaDataDirCfg.init(source)

    module = import_module(f"kgdata.dbpedia.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
