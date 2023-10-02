from __future__ import annotations

import gc
import os
import shutil
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast, get_args

import click
import orjson
import ray
import serde.jl
import serde.json
from loguru import logger
from timer import Timer

from hugedict.cachedict import CacheDict
from hugedict.prelude import (
    RocksDBDict,
    RocksDBOptions,
    init_env_logger,
    rocksdb_build_sst_file,
    rocksdb_ingest_sst_files,
    rocksdb_load,
)
from kgdata.config import init_dbdir_from_env
from kgdata.dataset import import_dataset
from kgdata.spark.extended_rdd import DatasetSignature
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.entity_metadata import entity_metadata
from kgdata.wikidata.datasets.property_count import property_count
from kgdata.wikidata.db import (
    WikidataDB,
    get_entity_label_db,
    get_ontcount_db,
    pack_int,
)
from kgdata.wikidata.extra_ent_db import EntAttr, build_extra_ent_db
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
from sm.misc.ray_helper import ray_map

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
    @click.option("-l", "--lang", default=None, help="Default language of the wikidata")
    def command(output: str, compact: bool, lang: Optional[str] = None):
        """Build a key-value database for storing dataset."""
        init_dbdir_from_env()

        def db_options():
            db = getattr(WikidataDB(output, read_only=False), dbname)
            while isinstance(db, CacheDict):
                db = db.mapping

            assert isinstance(db, RocksDBDict)
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

        ds = import_dataset("wikidata." + dataset, ds_kwargs)
        db_sig_file = Path(dbpath) / "_SIGNATURE"
        if db_sig_file.exists():
            db_sig = DatasetSignature.from_dict(serde.json.deser(db_sig_file))
            ds_sig = ds.get_signature()

            if db_sig != ds_sig:
                raise Exception(
                    "Trying to rebuild database, but the database already exists with different signature."
                )

            logger.info(
                "Database already exists with the same signature. Skip building."
            )
            return

        rocksdb_load(
            dbpath=dbpath,
            dbopts=options,
            files=ds.get_files(),
            format=fileformat,
            verbose=True,
            compact=compact,
        )
        serde.json.ser(ds.get_signature().to_dict(), db_sig_file)

    return command


@click.group()
def wikidata():
    pass


@click.command(name="entity_attr")
@click.option(
    "-a", "--attr", type=click.Choice(get_args(EntAttr)), help="Entity's attribute"
)
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_entities_attr(attr: EntAttr, output: str, compact: bool, lang: str):
    """Build a key-value database of Wikidata entities"""
    init_dbdir_from_env()

    dbpath = Path(output) / "entities_attr.db"
    build_extra_ent_db(dbpath, attr, lang=lang, compact=compact)


wikidata.add_command(dataset2db("entities", "entities"))
wikidata.add_command(
    dataset2db(
        "entity_metadata",
        format={
            "record_type": {"type": "ndjson", "key": "0", "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        "entity_types",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        "entity_labels",
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": "label"},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        "entity_pagerank",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
            "number_type": "f64",
        },
    )
)
wikidata.add_command(
    dataset2db(
        "entity_redirections",
        format={
            "record_type": {"type": "tabsep", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        "cross_wiki_mapping.default_cross_wiki_mapping",
        dbname="wp2wd",
        command_name="wp2wd",
        format={
            "record_type": {
                "type": "ndjson",
                "key": "wikipedia_title",
                "value": "wikidata_entityid",
            },
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        dataset="entity_outlinks",
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(dataset2db("classes"))
wikidata.add_command(dataset2db("properties", "props"))
wikidata.add_command(
    dataset2db(
        dataset="property_domains",
        dbname="prop_domains",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        dataset="property_ranges",
        dbname="prop_ranges",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        dataset="ontology_count",
        dbname="ontcount",
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
            "number_type": "u32",
        },
    )
)


@click.command(name="ontcount")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_ontcount(directory: str, output: str, compact: bool, lang: str):
    """Build a database storing the count of each ontology class & property"""
    WikidataDirCfg.init(directory)

    dbpath = Path(output) / "ontcount.db"
    dbpath.mkdir(exist_ok=True, parents=True)
    temp_dir = dbpath / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    options = cast(
        RocksDBDict,
        get_ontcount_db(dbpath, create_if_missing=True, read_only=False),
    ).options
    gc.collect()

    import ray

    from sm.misc.ray_helper import ray_map, ray_put

    ray.init()

    def _build_sst_file(
        infile: str, temp_dir: str, posfix: str, options: RocksDBOptions
    ):
        kvs = []
        for obj in serde.jl.deser(infile):
            kvs.append((obj[0].encode(), pack_int(obj[1])))
        kvs.sort(key=itemgetter(0))

        if len(kvs) == 0:
            return None

        tmp = {"counter": 0}

        def input_gen():
            if tmp["counter"] == len(kvs):
                return None
            obj = kvs[tmp["counter"]]
            tmp["counter"] += 1
            return obj

        outfile = os.path.join(temp_dir, Path(infile).stem + posfix + ".sst")
        assert not os.path.exists(outfile)
        rocksdb_build_sst_file(options, outfile, input_gen)
        assert os.path.exists(outfile)
        return outfile

    ray_opts = ray_put(options)
    with Timer().watch_and_report("Creating SST files"):
        sst_files = ray_map(
            _build_sst_file,
            [
                (file, str(temp_dir), "-c", ray_opts)
                for file in class_count().get_files()
            ]
            + [
                (file, str(temp_dir), "-p", ray_opts)
                for file in property_count().get_files()
            ],
            verbose=True,
            desc="Creating SST files",
            is_func_remote=False,
            using_ray=False,
        )

    with Timer().watch_and_report("Ingesting SST files"):
        rocksdb_ingest_sst_files(
            str(dbpath),
            options,
            [x for x in sst_files if x is not None],
            compact=compact,
        )


wikidata.add_command(db_entities_attr)
wikidata.add_command(db_ontcount)

if __name__ == "__main__":
    init_env_logger()
    wikidata()
