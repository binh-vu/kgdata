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
from kgdata.misc.query import PropEqualQuery, every


@click.command("Generate a specific dataset")
@click.option("-d", "--dataset", required=True, help="Dataset name")
@click.option("-t", "--take", type=int, required=False, default=0, help="Take n rows")
@click.option("-q", "--query", type=str, required=False, default="", help="Query")
def main(dataset: str, take: int = 0, query: str = ""):
    init_dbdir_from_env()
    module = import_module(f"kgdata.wikidata.datasets.{dataset}")
    ds = getattr(module, dataset)()

    if take > 0:
        for record in ds.take(take):
            print(record)
            print("=" * 30)

    if query != "":
        queries = [PropEqualQuery.from_string(s) for s in query.split(",")]
        filter_fn = every(queries)

        for record in ds.get_rdd().filter(filter_fn).collect():
            print(record)
            print("=" * 30)


if __name__ == "__main__":
    main()
