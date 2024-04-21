from __future__ import annotations

import gc
import os
import shutil
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Optional, cast, get_args

import click
import serde.jl
import serde.json
from hugedict.prelude import (
    RocksDBDict,
    RocksDBOptions,
    init_env_logger,
    rocksdb_build_sst_file,
    rocksdb_ingest_sst_files,
)
from kgdata.config import init_dbdir_from_env
from kgdata.db import build_database
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.class_count import class_count
from kgdata.wikidata.datasets.property_count import property_count
from kgdata.wikidata.db import WikidataDB, get_ontcount_db, pack_int
from kgdata.wikidata.extra_ent_db import EntAttr, build_extra_ent_db
from timer import Timer

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
    @click.option("-l", "--lang", default=None, help="Default language of the wikidata")
    def command(output: str, compact: bool, lang: Optional[str] = None):
        """Build a key-value database for storing dataset."""
        init_dbdir_from_env()
        build_database(
            f"kgdata.wikidata.datasets.{dataset}.{dataset}",
            lambda: getattr(WikidataDB(output, read_only=False), dbname),
            compact=compact,
            format=format,
            lang=lang,
        )

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
wikidata.add_command(
    dataset2db(
        dataset="mention_to_entities",
        dbname="mention_to_entities",
        format={
            "record_type": {
                "type": "ndjson",
                "key": "mention",
                "value": "target_entities",
            },
            "is_sorted": False,
        },
    )
)
wikidata.add_command(
    dataset2db(
        dataset="norm_mentions",
        dbname="norm_mentions",
        format={
            "record_type": {
                "type": "tuple2",
                "key": None,
                "value": None,
            },
            "is_sorted": False,
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
