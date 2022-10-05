import glob
import gc
from pathlib import Path
import shutil
from typing import List, cast, get_args
import click, os
from click.types import Choice
from hugedict.prelude import (
    RocksDBDict,
    RocksDBOptions,
    rocksdb_load,
    init_env_logger,
    rocksdb_build_sst_file,
    rocksdb_ingest_sst_files,
)
from hugedict.ray_parallel import ray_map
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_metadata import entity_metadata
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.datasets.properties import properties
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.property_domains import property_domains
from kgdata.wikidata.datasets.property_ranges import property_ranges
from kgdata.wikidata.datasets.wp2wd import wp2wd
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
from kgdata.wikidata.db import (
    get_entity_db,
    get_entity_label_db,
    get_entity_redirection_db,
    get_wdprop_domain_db,
    get_wdprop_range_db,
    get_wp2wd_db,
    get_wdclass_db,
    get_wdprop_db,
)
from kgdata.wikidata.extra_ent_db import build_extra_ent_db, EntAttr
from loguru import logger
import orjson
from operator import itemgetter

from sm.misc.funcs import identity_func
from sm.misc.deser import deserialize_jl
from sm.misc.timer import Timer
import ray


@click.command(name="entities")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_entities(directory: str, output: str, compact: bool, lang: str):
    """Build a key-value database of Wikidata entities"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdentities.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    options = cast(RocksDBDict, get_entity_db(dbpath)).options
    gc.collect()

    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=options,
        files=entities(lang=lang).get_files(),
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=compact,
    )


@click.command(name="entity_attr")
@click.option("-d", "--directory", default="", help="Wikidata directory")
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
def db_entities_attr(
    directory: str, attr: EntAttr, output: str, compact: bool, lang: str
):
    """Build a key-value database of Wikidata entities"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdentities_attr.db"
    build_extra_ent_db(dbpath, attr, lang=lang, compact=compact)


@click.command(name="entity_labels")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_entity_labels(directory: str, output: str, compact: bool, lang: str):
    """Wikidata entity labels"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdentity_labels.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    temp_dir = dbpath / "_temporary"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    options = cast(
        RocksDBDict,
        get_entity_label_db(dbpath, create_if_missing=True, read_only=False),
    ).options
    gc.collect()

    ray.init()

    @ray.remote
    def _build_sst_file(infile: str, temp_dir: str, options: RocksDBOptions):
        kvs = sorted(
            [
                (
                    obj["id"].encode(),
                    orjson.dumps(WDEntityLabel.from_wdentity_raw(obj).to_dict()),
                )
                for obj in deserialize_jl(infile)
            ],
            key=itemgetter(0),
        )
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
        sst_files = ray_map(
            _build_sst_file.remote,
            [
                (file, str(temp_dir), ray_opts)
                for file in entity_metadata(lang=lang).get_files()
            ],
            verbose=True,
        )

    with Timer().watch_and_report("Ingesting SST files"):
        rocksdb_ingest_sst_files(str(dbpath), options, sst_files, True)


@click.command(name="entity_redirections")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
def db_entity_redirections(directory: str, output: str, compact: bool):
    """Wikidata entity redirections"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdentity_redirections.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    options = cast(
        RocksDBDict,
        get_entity_redirection_db(dbpath, create_if_missing=True, read_only=False),
    ).options
    gc.collect()

    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=options,
        files=entity_redirections().get_files(),
        format={
            "record_type": {"type": "tabsep", "key": None, "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=True,
    )


@click.command(name="classes")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_classes(directory: str, output: str, compact: bool, lang: str):
    """Wikidata classes"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdclasses.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    options = cast(
        RocksDBDict,
        get_wdclass_db(dbpath),
    ).options
    gc.collect()

    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=options,
        files=classes(lang=lang).get_files(),
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=True,
    )


@click.command(name="properties")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-e",
    "--extra",
    type=Choice(["domains", "ranges"], case_sensitive=False),
    multiple=True,
)
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_properties(
    directory: str, output: str, extra: List[str], compact: bool, lang: str
):
    """Build databases storing Wikidata properties. It comes with a list of extra
    options (sub databases) for building domains and ranges of properties.
    """
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wdprops.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    options = cast(
        RocksDBDict,
        get_wdprop_db(dbpath),
    ).options
    gc.collect()

    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=options,
        files=properties(lang=lang).get_files(),
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=True,
    )

    for name in extra:
        if name == "domains":
            dbpath = Path(output) / "wdprop_domains.db"
            dbpath.mkdir(exist_ok=True, parents=True)
            files = property_domains(lang=lang).get_files()
            options = cast(
                RocksDBDict,
                get_wdprop_domain_db(dbpath, create_if_missing=True, read_only=False),
            ).options
            gc.collect()
        elif name == "ranges":
            dbpath = Path(output) / "wdprop_ranges.db"
            dbpath.mkdir(exist_ok=True, parents=True)
            files = property_ranges(lang=lang).get_files()
            options = cast(
                RocksDBDict,
                get_wdprop_range_db(dbpath, create_if_missing=True, read_only=False),
            ).options
            gc.collect()
        else:
            raise NotImplementedError(name)

        rocksdb_load(
            dbpath=str(dbpath),
            dbopts=options,
            files=files,
            format={
                "record_type": {"type": "tuple2", "key": None, "value": None},
                "is_sorted": False,
            },
            verbose=True,
            compact=True,
        )


@click.command(name="wp2wd")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def db_wp2wd(directory: str, output: str, compact: bool, lang: str):
    """Mapping from Wikipedia articles to Wikidata entities"""
    WDDataDirCfg.init(directory)

    dbpath = Path(output) / "wp2wd.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    options = cast(
        RocksDBDict,
        get_wp2wd_db(dbpath, create_if_missing=True, read_only=False),
    ).options
    gc.collect()

    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=options,
        files=wp2wd(lang=lang).get_files(),
        format={
            "record_type": {"type": "tuple2", "key": None, "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=True,
    )


@click.command(name="search.entities")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def search_entities(directory: str, output: str, lang: str):
    from kgdata.wikidata.search import build_index

    WDDataDirCfg.init(directory)
    build_index(name="entities", index_parent_dir=Path(output), lang=lang)


@click.command(name="search.properties")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def search_props(directory: str, output: str, lang: str):
    from kgdata.wikidata.search import build_index

    WDDataDirCfg.init(directory)
    build_index(name="props", index_parent_dir=Path(output), lang=lang)


@click.command(name="search.classes")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option("-l", "--lang", default="en", help="Default language of the Wikidata")
def search_classes(directory: str, output: str, lang: str):
    from kgdata.wikidata.search import build_index

    WDDataDirCfg.init(directory)
    build_index(name="classes", index_parent_dir=Path(output), lang=lang)


@click.group()
def wikidata():
    pass


wikidata.add_command(db_entities)
wikidata.add_command(db_entities_attr)
wikidata.add_command(db_entity_labels)
wikidata.add_command(db_entity_redirections)
wikidata.add_command(db_classes)
wikidata.add_command(db_properties)
wikidata.add_command(db_wp2wd)
wikidata.add_command(search_entities)
wikidata.add_command(search_props)
wikidata.add_command(search_classes)


if __name__ == "__main__":
    init_env_logger()
    wikidata()
