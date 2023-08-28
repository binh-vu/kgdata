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
from kgdata.misc.query import PropQuery, every


@click.command("Generate a specific dataset")
@click.option("-d", "--dataset", required=True, help="Dataset name")
@click.option("-t", "--take", type=int, required=False, default=0, help="Take n rows")
@click.option("-q", "--query", type=str, required=False, default="", help="Query")
@click.option("-l", "--limit", type=int, required=False, default=20, help="Limit")
@click.option(
    "-s",
    "--sign",
    is_flag=True,
    required=False,
    default=False,
    help="Sign the dataset if it hasn't been signed",
)
def main(
    dataset: str, take: int = 0, query: str = "", limit: int = 20, sign: bool = False
):
    init_dbdir_from_env()

    module = import_module(f"kgdata.dbpedia.datasets.{dataset}")
    ds = getattr(module, dataset)()

    if take > 0:
        for record in ds.take(take):
            print(repr(record))
            print("=" * 30)

    if query != "":
        queries = [PropQuery.from_string(s) for s in query.split(",")]
        filter_fn = every(queries)

        for record in ds.get_rdd().filter(filter_fn).take(limit):
            print(repr(record))
            print("=" * 30)

    if sign:
        for dep in ds.get_dependencies():
            # make sure that the dependencies are all signed
            try:
                dep.get_signature()
            except:
                print(f"{dep.get_name()} doesn't have a signature")
                if dep.get_name() in [
                    "entities/merge-en",
                    "ontology-dump/20230420/step-2",
                ]:
                    dep.sign(dep.get_name(), dep.get_dependencies(), checksum=False)
                else:
                    raise

        ds.sign(
            ds.get_name(),
            ds.get_dependencies(),
        )


if __name__ == "__main__":
    main()
