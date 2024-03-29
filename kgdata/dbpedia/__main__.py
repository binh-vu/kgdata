from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click
from hugedict.prelude import init_env_logger
from kgdata.config import init_dbdir_from_env
from kgdata.db import GenericDB, build_database

if TYPE_CHECKING:
    from hugedict.core.rocksdb import FileFormat


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
        build_database(
            f"kgdata.dbpedia.datasets.{dataset}.{dataset}",
            lambda: getattr(GenericDB(output, read_only=False), dbname),
            compact=compact,
            format=format,
            lang=lang,
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
        "entity_metadata",
        format={
            "record_type": {"type": "ndjson", "key": "0", "value": None},
            "is_sorted": False,
        },
    )
)
dbpedia.add_command(
    dataset2db(
        "entity_labels",
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": "label"},
            "is_sorted": False,
        },
    )
)
dbpedia.add_command(
    dataset2db(
        "entity_redirections",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)
dbpedia.add_command(
    dataset2db(
        "entity_types",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)

if __name__ == "__main__":
    init_env_logger()
    dbpedia()
