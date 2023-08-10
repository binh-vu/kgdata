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

import click
from loguru import logger
from typing_extensions import Required

from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikipedia.config import WikipediaDirCfg


@click.command("Generate a specific dataset")
@click.option("--wp-dir", required=True, help="Wikipedia directory")
@click.option("--wd-dir", default="", help="Wikidata directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
@click.option("-t", "--take", type=int, required=False, default=0, help="Take n rows")
def main(wp_dir: str, wd_dir: str, dataset: str, take: int = 0):
    logger.info("Wikipedia directory: {}", wp_dir)

    WikipediaDirCfg.init(wp_dir)
    if wd_dir.strip() != "":
        logger.info("Wikidata directory: {}", wd_dir)
        WikidataDirCfg.init(wd_dir)

    module = import_module(f"kgdata.wikipedia.datasets.{dataset}")
    ds = getattr(module, dataset)()

    if take > 0:
        for record in ds.take(take):
            print(record)
            print("=" * 30)


if __name__ == "__main__":
    main()
