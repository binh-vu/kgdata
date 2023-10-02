from __future__ import annotations

from functools import cached_property, partial
from pathlib import Path
from typing import TypeVar

from kgdata.db import (
    deser_from_dict,
    get_rocksdb,
    large_dbopts,
    ser_to_dict,
    small_dbopts,
)
from kgdata.models.entity import Entity
from kgdata.models.ont_class import OntologyClass
from kgdata.models.ont_property import OntologyProperty

T = TypeVar("T")

get_entity_db = partial(
    get_rocksdb,
    deser_value=partial(deser_from_dict, Entity),
    ser_value=ser_to_dict,
    dbopts=large_dbopts,
)
get_entity_label_db = partial(
    get_rocksdb,
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)
get_entity_redirection_db = partial(
    get_rocksdb,
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)
get_class_db = partial(
    get_rocksdb,
    deser_value=partial(deser_from_dict, OntologyClass),
    ser_value=ser_to_dict,
    dbopts=small_dbopts,
)
get_prop_db = partial(
    get_rocksdb,
    deser_value=partial(deser_from_dict, OntologyProperty),
    ser_value=ser_to_dict,
    dbopts=small_dbopts,
)
get_redirection_db = partial(
    get_rocksdb,
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)


class DBpediaDB:
    instance = None

    def __init__(self, database_dir: Path | str, read_only: bool = True):
        self.database_dir = Path(database_dir)
        self.read_only = read_only

    @cached_property
    def entities(self):
        return get_entity_db(
            self.database_dir / "entities.db", read_only=self.read_only
        )

    @cached_property
    def entity_labels(self):
        return get_entity_label_db(
            self.database_dir / "entity_labels.db", read_only=self.read_only
        )

    @cached_property
    def entity_redirections(self):
        return get_entity_redirection_db(
            self.database_dir / "entity_redirections.db", read_only=self.read_only
        )

    @cached_property
    def classes(self):
        return get_class_db(self.database_dir / "classes.db", read_only=self.read_only)

    @cached_property
    def props(self):
        return get_prop_db(self.database_dir / "props.db", read_only=self.read_only)

    @cached_property
    def redirections(self):
        return get_redirection_db(
            self.database_dir / "redirections.db", read_only=self.read_only
        )


if __name__ == "__main__":
    import click

    @click.command()
    @click.option("-d", "--data-dir", required=True, help="database directory")
    @click.option("-n", "--dbname", required=True, help="database name")
    @click.argument("keys", nargs=-1)
    def cli(data_dir: str, dbname: str, keys: list[str]):
        db = getattr(DBpediaDB(data_dir), dbname)
        if len(keys) == 0:
            print(len(db))
            it = iter(db.keys())
            # for i in range(200):
            #     next(it)
            for i in range(2):
                k = next(it)
                print("key:", k)
                print("value:", db[k])
        else:
            for k in keys:
                print("key:", k)
                print("value:", db[k])
                print("")

    cli()
