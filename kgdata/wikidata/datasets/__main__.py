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

import click

from kgdata.config import init_dbdir_from_env


@click.command("Generate a specific dataset")
@click.option("-d", "--dataset", required=True, help="Dataset name")
def main(dataset: str):
    init_dbdir_from_env()
    module = import_module(f"kgdata.wikidata.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
