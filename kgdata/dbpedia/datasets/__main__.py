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

import click

from kgdata.config import init_dbdir_from_env


@click.command("Generate a specific dataset")
@click.option("-d", "--dataset", required=True, help="Dataset name")
@click.option("-t", "--take", type=int, required=False, default=0, help="Take n rows")
def main(dataset: str, take: int = 0):
    init_dbdir_from_env()

    module = import_module(f"kgdata.dbpedia.datasets.{dataset}")
    ds = getattr(module, dataset)()

    if take > 0:
        for record in ds.take(take):
            print(record)
            print("=" * 30)


if __name__ == "__main__":
    main()
