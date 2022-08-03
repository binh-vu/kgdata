"""
Run the command ``python -m kgdata.wikipedia.datasets`` to generate the datasets.

.. code:: bash

    $ python -m kgdata.wikipedia.datasets --help

    Usage: python -m kgdata.wikipedia.datasets [OPTIONS]

    Options:
    --wp-dir TEXT       Wikipedia directory  [required]
    --wd-dir TEXT       Wikidata directory
    -d, --dataset TEXT  Dataset name  [required]
    --help              Show this message and exit. 

Examples::

    python -m kgdata.wikipedia.datasets --wp-dir $WP_DIR -d relational_tables
    python -m kgdata.wikipedia.datasets --wp-dir $WP_DIR --wd-dir $WD_DIR -d linked_relational_tables

.. note::

    For the commands in the above examples to run correctly, replaced ``$WP_DIR`` with the path to the Wikipedia directory and ``$WD_DIR`` with the path to the Wikidata directory, e.g., ``export WD_DIR=/data/wikipedia/20220420``

"""

from importlib import import_module
from typing_extensions import Required
import click
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikidata.config import WDDataDirCfg
import kgdata.wikipedia.datasets
from loguru import logger


@click.command("Generate a specific dataset")
@click.option("--wp-dir", required=True, help="Wikipedia directory")
@click.option("--wd-dir", default="", help="Wikidata directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
def main(wp_dir: str, wd_dir: str, dataset: str):
    logger.info("Wikipedia directory: {}", wp_dir)

    WPDataDirConfig.init(wp_dir)
    if wd_dir.strip() != "":
        logger.info("Wikidata directory: {}", wd_dir)
        WDDataDirCfg.init(wd_dir)

    module = import_module(f"kgdata.wikipedia.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
