"""
Helper for creating rocksdb entity/class/prop databases.
"""

from __future__ import annotations

import struct
from functools import partial
from pathlib import Path
from typing import Callable, Generic, TypeVar

import orjson
import serde.json
from hugedict.prelude import RocksDBCompressionOptions, RocksDBDict, RocksDBOptions
from hugedict.types import HugeMutableMapping
from kgdata.models.entity import Entity
from kgdata.models.ont_class import OntologyClass
from kgdata.models.ont_property import OntologyProperty

T = TypeVar("T")


small_dbopts = dict(
    compression_type="lz4",
)
medium_dbopts = dict(
    compression_type="lz4",
    bottommost_compression_type="zstd",
)
large_dbopts = dict(
    compression_type="zstd",
    compression_opts=RocksDBCompressionOptions(
        window_bits=-14, level=6, strategy=0, max_dict_bytes=16 * 1024
    ),
    zstd_max_train_bytes=100 * 16 * 1024,
)


class make_get_rocksdb(Generic[T]):
    def __init__(
        self,
        *,
        ser_value: Callable[[T], bytes],
        deser_value: Callable[[bytes], T],
        version: str | int = 1,
        dbopts: dict | None = None,
    ):
        self.ser_value = ser_value
        self.deser_value = deser_value
        self.version = version
        self.dbopts = dbopts

    def __call__(
        self,
        dbfile: Path | str,
        *,
        cls: type[RocksDBDict] = RocksDBDict,
        create_if_missing: bool = True,
        read_only: bool = False,
    ) -> HugeMutableMapping[str, T]:
        version_file = Path(dbfile) / "_VERSION"
        if version_file.exists():
            obj = serde.json.deser(version_file)
            assert obj["version"] == self.version, obj
        else:
            version_file.parent.mkdir(parents=True, exist_ok=True)
            serde.json.ser(
                {
                    "version": self.version,
                    "opts": {
                        k: v if isinstance(v, (str, int)) else v.to_dict()
                        for k, v in self.dbopts.items()
                    }
                    if self.dbopts is not None
                    else None,
                },
                version_file,
            )

        rocksdbopts = RocksDBOptions(**self.dbopts, create_if_missing=create_if_missing)  # type: ignore
        return cls(
            path=str(dbfile),
            options=rocksdbopts,
            readonly=read_only,
            deser_key=partial(str, encoding="utf-8"),
            deser_value=self.deser_value,
            ser_value=self.ser_value,
        )


def get_rocksdb(
    dbfile: Path | str,
    *,
    ser_value: Callable[[T], bytes],
    deser_value: Callable[[bytes], T],
    cls: type[RocksDBDict] = RocksDBDict,
    create_if_missing: bool = True,
    read_only: bool = False,
    dbopts: dict | None = None,
    version: str | int = 1,
) -> HugeMutableMapping[str, T]:
    return make_get_rocksdb(
        ser_value=ser_value, deser_value=deser_value, dbopts=dbopts, version=version
    )(
        dbfile,
        cls=cls,
        create_if_missing=create_if_missing,
        read_only=read_only,
    )


def deser_from_dict(cls: type[T], data: bytes | str) -> T:
    return cls.from_dict(orjson.loads(data))  # type: ignore


def deser_from_tuple(cls: type[T], data: bytes | str) -> T:
    return cls.from_tuple(orjson.loads(data))  # type: ignore


def ser_to_dict(value: T) -> bytes:  # type: ignore
    return orjson.dumps(value.to_dict())  # type: ignore


def ser_to_tuple(value: T) -> bytes:  # type: ignore
    return orjson.dumps(value.to_tuple())  # type: ignore


def pack_int(v: int) -> bytes:
    return struct.pack("<l", v)


def unpack_int(v: bytes) -> int:
    return struct.unpack("<l", v)[0]


def pack_float(v: float) -> bytes:
    return struct.pack("<d", v)


def unpack_float(v: bytes) -> float:
    return struct.unpack("<d", v)[0]


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


class GenericDB:
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

    @cached_property
    def entity_pagerank(self):
        raise NotImplementedError()

    @cached_property
    def entity_metadata(self):
        raise NotImplementedError()

    @cached_property
    def entity_types(self):
        raise NotImplementedError()

    @cached_property
    def ontcount(self):
        raise NotImplementedError()
