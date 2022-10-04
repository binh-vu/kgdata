"""Wikidata embedded key-value databases for each entity's attribute."""

from functools import partial
import gc, struct
from operator import itemgetter
import os
from pathlib import Path
import shutil
from typing import (
    Dict,
    Literal,
    Union,
    TypeVar,
    Optional,
    Callable,
    Set,
    List,
    Type,
    cast,
    overload,
)
from hugedict.prelude import (
    RocksDBDict,
    RocksDBOptions,
    rocksdb_load,
    init_env_logger,
    rocksdb_build_sst_file,
    rocksdb_ingest_sst_files,
)
from hugedict.ray_parallel import ray_map
from hugedict.types import HugeMutableMapping
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_metadata import (
    convert_to_entity_metadata,
    deser_entity_metadata,
    entity_metadata,
    ser_entity_metadata,
)
from kgdata.wikidata.datasets.entity_pagerank import entity_pagerank
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
from kgdata.wikidata.models.wdentitymetadata import WDEntityMetadata
from numpy import str0
import orjson
from hugedict.misc import zstd6_compress_custom, zstd_decompress_custom, identity
import requests

from hugedict.prelude import RocksDBDict, RocksDBOptions, RocksDBCompressionOptions
from kgdata.wikidata.models import (
    WDClass,
    WDProperty,
    WDPropertyRanges,
    WDPropertyDomains,
)
from sm.misc.deser import deserialize_byte_lines, deserialize_jl, deserialize_lines
from sm.misc.timer import Timer


EntAttr = Literal["label", "description", "aliases", "instanceof", "pagerank"]


def get_multilingual_key(id: str, lang: str) -> str:
    return f"{id}_{lang}"


@overload
def get_entity_attr_db(
    dbfile: Union[Path, str],
    attr: Literal["label", "description"],
    create_if_missing=True,
    read_only=False,
) -> HugeMutableMapping[str, str]:
    ...


@overload
def get_entity_attr_db(
    dbfile: Union[Path, str],
    attr: Literal["aliases", "instanceof"],
    create_if_missing=True,
    read_only=False,
) -> HugeMutableMapping[str, List[str]]:
    ...


@overload
def get_entity_attr_db(
    dbfile: Union[Path, str],
    attr: Literal["pagerank"],
    create_if_missing=True,
    read_only=False,
) -> HugeMutableMapping[str, float]:
    ...


def get_entity_attr_db(
    dbfile: Union[Path, str],
    attr: EntAttr,
    create_if_missing=True,
    read_only=False,
) -> Union[
    HugeMutableMapping[str, str],
    HugeMutableMapping[str, float],
    HugeMutableMapping[str, List[str]],
]:
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
        deser_value = partial(struct.unpack, "<d")
        ser_value = partial(struct.pack, "<d")
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


def build_extra_ent_db(
    dbpath: Path, attr: EntAttr, lang: str = "en", compact: bool = True
):
    realdbpath = dbpath.parent / dbpath.stem / f"{attr}.db"
    temp_dir = realdbpath / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    options = cast(RocksDBDict, get_entity_attr_db(dbpath, attr)).options
    gc.collect()

    import ray

    ray.init()

    @ray.remote
    def _build_sst_file(
        infile: str, temp_dir: str, attr: EntAttr, options: RocksDBOptions
    ):
        kvs = []
        if attr == "pagerank":
            for line in deserialize_lines(infile):
                ent_id, rank = line.split("\t")
                # double little-endian
                rank = struct.pack("<d", float(rank))
                kvs.append((ent_id.encode(), rank))
        else:
            for obj in deserialize_jl(infile):
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

    ray_opts = ray.put(options)
    with Timer().watch_and_report("Creating SST files"):
        if attr == "pagerank":
            dataset = entity_pagerank(lang=lang)
        else:
            dataset = entity_metadata(lang=lang)

        sst_files = ray_map(
            _build_sst_file.remote,
            [(file, str(temp_dir), attr, ray_opts) for file in dataset.get_files()],
            verbose=True,
        )

    with Timer().watch_and_report("Ingesting SST files"):
        rocksdb_ingest_sst_files(str(realdbpath), options, sst_files, compact=compact)
