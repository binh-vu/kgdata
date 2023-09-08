from __future__ import annotations

import gc
from typing import TYPE_CHECKING, Optional

import click
from hugedict.prelude import RocksDBDict, init_env_logger, rocksdb_load

from kgdata.config import init_dbdir_from_env
from kgdata.dataset import import_dataset
from kgdata.dbpedia.db import DBpediaDB

if TYPE_CHECKING:
    from hugedict.hugedict.rocksdb import FileFormat


def dataset2db(
    dataset: str,
    dbname: Optional[str] = None,
    format: Optional[FileFormat] = None,
    command_name: Optional[str] = None,
):
    if dbname is None:
        dbname = dataset

    @click.command(name=command_name or dataset)
    @click.option("-o", "--output", help="Output directory")
    @click.option(
        "-c",
        "--compact",
        is_flag=True,
        help="Whether to compact the results. May take a very very long time",
    )
    @click.option("-l", "--lang", default=None, help="Default language of the dbpedia")
    def command(output: str, compact: bool, lang: Optional[str] = None):
        """Build a key-value database for storing dataset."""
        init_dbdir_from_env()

        def db_options():
            db: RocksDBDict = getattr(DBpediaDB(output, read_only=False), dbname)
            return db.options, db.path

        options, dbpath = db_options()
        gc.collect()

        fileformat = format or {
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        }

        ds_kwargs = {}
        if lang is not None:
            ds_kwargs["lang"] = lang

        rocksdb_load(
            dbpath=dbpath,
            dbopts=options,
            files=import_dataset("dbpedia." + dataset, ds_kwargs).get_files(),
            format=fileformat,
            verbose=True,
            compact=compact,
        )

    return command


@click.group()
def dbpedia():
    pass


dbpedia.add_command(dataset2db("entities"))
dbpedia.add_command(dataset2db("classes"))
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
