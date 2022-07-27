"""
Run the command ``python -m kgdata.wikipedia.datasets`` to generate the datasets.

.. code:: bash

    $ python -m kgdata.wikipedia.datasets --help

    Usage: python -m kgdata.wikipedia.datasets [OPTIONS]

    Options:
    -s, --source TEXT   Wikipedia directory  [required]
    -d, --dataset TEXT  Dataset name  [required]
    --help              Show this message and exit. 

Examples::

    python -m kgdata.wikipedia.datasets -s $WP_DIR -d relational_tables

.. note::

    For the commands in the above examples to run correctly, replaced ``$WP_DIR`` with the path to the Wikipedia directory, e.g., ``export WD_DIR=/data/wikipedia/20220420``

"""

from importlib import import_module
from typing_extensions import Required
import click
from kgdata.wikipedia.config import WPDataDirConfig
import kgdata.wikipedia.datasets
from loguru import logger


@click.command("Generate a specific dataset")
@click.option("-s", "--source", required=True, help="Wikipedia directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
def main(source: str, dataset: str):
    logger.info("Wikidata directory: {}", source)

    WPDataDirConfig.init(source)

    module = import_module(f"kgdata.wikipedia.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
