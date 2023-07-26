from __future__ import annotations

import gc
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, cast

import click
from click.types import Choice
from hugedict.prelude import RocksDBDict, init_env_logger, rocksdb_load
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets import import_dataset
from kgdata.dbpedia.db import DBpediaDB

if TYPE_CHECKING:
    from hugedict.hugedict.rocksdb import FileFormat


def dataset2db(
    dataset: str,
    dbname: str,
    format: Optional[FileFormat] = None,
    command_name: Optional[str] = None,
):
    @click.command(name=command_name or dataset)
    @click.option("-d", "--directory", default="", help="dbpedia directory")
    @click.option("-o", "--output", help="Output directory")
    @click.option(
        "-e",
        "--extra",
        type=Choice([], case_sensitive=False),
        multiple=True,
    )
    @click.option(
        "-c",
        "--compact",
        is_flag=True,
        help="Whether to compact the results. May take a very very long time",
    )
    @click.option("-l", "--lang", default="en", help="Default language of the dbpedia")
    def command(
        directory: str, output: str, extra: List[str], compact: bool, lang: str
    ):
        """Build databases storing DBpedia properties. It comes with a list of extra
        options (sub databases) for building domains and ranges of properties.
        """
        DBpediaDirCfg.init(directory)

        def db_options():
            db: RocksDBDict = getattr(DBpediaDB(output, read_only=False), dbname)
            return db.options, db.path

        options, dbpath = db_options()
        gc.collect()

        fileformat = format or {
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        }

        rocksdb_load(
            dbpath=dbpath,
            dbopts=options,
            files=import_dataset(dataset).get_files(),
            format=fileformat,
            verbose=True,
            compact=compact,
        )

    return command


@click.group()
def dbpedia():
    pass


dbpedia.add_command(dataset2db("entities", "entities"))
dbpedia.add_command(dataset2db("classes", "classes"))
dbpedia.add_command(dataset2db("properties", "props"))
dbpedia.add_command(
    dataset2db(
        "redirection_dump",
        "redirections",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
        command_name="redirections",
    )
)

if __name__ == "__main__":
    init_env_logger()
    dbpedia()
