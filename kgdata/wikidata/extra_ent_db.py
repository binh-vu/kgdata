"""Wikidata embedded key-value databases for each entity's attribute."""

from __future__ import annotations

import gc
import os
import shutil
import struct
from functools import partial
from operator import itemgetter
from pathlib import Path
from typing import Literal, TypedDict, cast, overload

import orjson
import serde.jl
from timer import Timer

from hugedict.prelude import (
    RocksDBCompressionOptions,
    RocksDBDict,
    RocksDBOptions,
    rocksdb_build_sst_file,
    rocksdb_ingest_sst_files,
)
from hugedict.types import HugeMutableMapping
from kgdata.wikidata.models.wdentitymetadata import WDEntityMetadata

EntAttr = Literal["label", "description", "aliases", "instanceof", "pagerank"]
PageRankStats = TypedDict(
    "PageRankStats",
    {"min": float, "max": float, "mean": float, "std": float, "sum": float, "len": int},
)


def get_multilingual_key(id: str, lang: str) -> str:
    return f"{id}_{lang}"


def pack_float(v: float) -> bytes:
    return struct.pack("<d", v)


def unpack_float(v: bytes) -> float:
    return struct.unpack("<d", v)[0]


@overload
def get_entity_attr_db(
    dbfile: Path | str,
    attr: Literal["label", "description"],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, str]:
    ...


@overload
def get_entity_attr_db(
    dbfile: Path | str,
    attr: Literal["aliases", "instanceof"],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, list[str]]:
    ...


@overload
def get_entity_attr_db(
    dbfile: Path | str,
    attr: Literal["pagerank"],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, float]:
    ...


def get_entity_attr_db(
    dbfile: Path | str,
    attr: EntAttr,
    create_if_missing=False,
    read_only=True,
) -> (
    HugeMutableMapping[str, str]
    | HugeMutableMapping[str, float]
    | HugeMutableMapping[str, list[str]]
):
    dbpath = Path(dbfile)
    realdbpath = dbpath.parent / dbpath.stem / f"{attr}.db"
    version = realdbpath / "_VERSION"

    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

    if attr == "aliases" or attr == "instanceof":
        deser_value = orjson.loads
        ser_value = orjson.dumps
    elif attr == "pagerank":
        deser_value = unpack_float
        ser_value = pack_float
    else:
        deser_value = partial(str, encoding="utf-8")
        ser_value = str.encode

    db = RocksDBDict(
        path=str(realdbpath),
        options=RocksDBOptions(
            create_if_missing=create_if_missing,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=-14, level=6, strategy=0, max_dict_bytes=16 * 1024
            ),
            zstd_max_train_bytes=100 * 16 * 1024,
        ),
        readonly=read_only,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=deser_value,
        ser_value=ser_value,  # type: ignore
    )

    return db


def get_pagerank_stats(
    dbfile: Path | str,
) -> PageRankStats:
    dbpath = Path(dbfile)
    realdbpath = dbpath.parent / dbpath.stem / f"pagerank.db"
    return orjson.loads((realdbpath / "pagerank_stats.json").read_bytes())


def build_extra_ent_db(
    dbpath: Path, attr: EntAttr, lang: str = "en", compact: bool = True
):
    realdbpath = dbpath.parent / dbpath.stem / f"{attr}.db"
    temp_dir = realdbpath / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    options = cast(
        RocksDBDict,
        get_entity_attr_db(dbpath, attr, create_if_missing=True, read_only=False),
    ).options
    gc.collect()


    from kgdata.wikidata.datasets.entity_metadata import entity_metadata
    from kgdata.wikidata.datasets.entity_pagerank import entity_pagerank
    from sm.misc.ray_helper import ray_map, ray_put

    def _build_sst_file(
        infile: str, temp_dir: str, attr: EntAttr, options: RocksDBOptions
    ):
        kvs = []
        for obj in serde.jl.deser(infile):
            if attr == "pagerank":
                ent_id, rank = obj
                rank = struct.pack("<d", float(rank))
                kvs.append((ent_id.encode(), rank))
                continue

            ent = WDEntityMetadata.from_tuple(obj)
            if attr == "label" or attr == "description":
                for lang, value in getattr(ent, attr).lang2value.items():
                    kvs.append(
                        (
                            get_multilingual_key(ent.id, lang).encode(),
                            value.encode(),
                        )
                    )
            elif attr == "aliases":
                for lang, values in getattr(ent, attr).lang2values.items():
                    kvs.append(
                        (
                            get_multilingual_key(ent.id, lang).encode(),
                            orjson.dumps(values),
                        )
                    )
            elif attr == "instanceof":
                kvs.append((ent.id.encode(), orjson.dumps(ent.instanceof)))
            else:
                raise NotImplementedError(attr)
        kvs.sort(key=itemgetter(0))
        tmp = {"counter": 0}

        def input_gen():
            if tmp["counter"] == len(kvs):
                return None
            obj = kvs[tmp["counter"]]
            tmp["counter"] += 1
            return obj

        outfile = os.path.join(temp_dir, Path(infile).stem + ".sst")
        rocksdb_build_sst_file(options, outfile, input_gen)
        return outfile

    ray_opts = ray_put(options)
    with Timer().watch_and_report("Creating SST files"):
        if attr == "pagerank":
            dataset = entity_pagerank(lang=lang)
            # copy the statistics file to the temporary directory
            dataset_dir = Path(dataset.file_pattern).parent
            assert dataset_dir.exists()
            statsfile = dataset_dir.parent / f"{dataset_dir.name}.pkl"
            assert statsfile.exists(), str(statsfile)
            shutil.copy2(statsfile, realdbpath / "pagerank_stats.pkl")
        else:
            dataset = entity_metadata(lang=lang)

        sst_files = ray_map(
            _build_sst_file,
            [(file, str(temp_dir), attr, ray_opts) for file in dataset.get_files()],
            verbose=True,
            is_func_remote=False,
        )

    with Timer().watch_and_report("Ingesting SST files"):
        rocksdb_ingest_sst_files(str(realdbpath), options, sst_files, compact=compact)
