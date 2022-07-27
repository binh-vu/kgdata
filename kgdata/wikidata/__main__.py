import glob
from pathlib import Path
from typing import List
import click, os
from click.types import Choice
from hugedict.prelude import rocksdb_load
from hugedict.misc import Chain2, zstd6_compress, Chain3
from kgdata.wikidata.config import WDDataDirCfg
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
    get_wp2wd_db,
    get_wdclass_db,
    get_wdprop_db,
)
from loguru import logger
import orjson
from operator import itemgetter

from sm.misc.funcs import identity_func


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

    dbopts = get_entity_db(dbpath).dbopts
    rocksdb_load(
        dbpath=str(dbpath),
        dbopts=dbopts,
        infiles=entities(lang=lang).get_files(),
        format={
            "record_type": {"type": "ndjson", "key": "id", "value": None},
            "is_sorted": False,
        },
        verbose=True,
        compact=True,
    )


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

    db = load(
        db=get_entity_label_db(dbpath, create_if_missing=True, read_only=False).db,
        infiles=entities(lang=lang).get_files(),
        format=FileFormat.jsonline,
        key_fn=Chain2(str.encode, itemgetter("id")).exec,
        value_fn=Chain3(
            orjson.dumps, WDEntityLabel.to_dict, WDEntityLabel.from_wdentity_raw
        ).exec,
        n_processes=8,
        shm_mem_ratio=12,
        shm_mem_limit_mb=128,
    )
    if compact:
        logger.info("Run compaction...")
        db.compact_range()


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

    db = load(
        db=get_entity_redirection_db(
            dbpath, create_if_missing=True, read_only=False
        ).db,
        infiles=entity_redirections().get_files(),
        format=FileFormat.tabsep,
        key_fn=identity_func,
        value_fn=identity_func,
        n_processes=8,
        shm_mem_ratio=12,
        shm_mem_limit_mb=128,
    )
    if compact:
        logger.info("Run compaction...")
        db.compact_range()


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

    db = load(
        db=get_wdclass_db(dbpath).db,
        infiles=classes(lang=lang).get_files(),
        format=FileFormat.jsonline,
        key_fn=Chain2(str.encode, itemgetter("id")).exec,
        value_fn=orjson.dumps,
        n_processes=8,
        shm_mem_ratio=12,
        shm_mem_limit_mb=128,
    )
    if compact:
        logger.info("Run compaction...")
        db.compact_range()


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

    db = load(
        db=get_wdprop_db(dbpath).db,
        infiles=properties(lang=lang).get_files(),
        format=FileFormat.jsonline,
        key_fn=Chain2(str.encode, itemgetter("id")).exec,
        value_fn=orjson.dumps,
        n_processes=8,
        shm_mem_ratio=12,
        shm_mem_limit_mb=128,
    )
    if compact:
        logger.info("Compacting wdprops.db...")
        db.compact_range()

    for name in extra:
        if name == "domains":
            dbpath = Path(output) / "wdprop_domains.db"
            dbpath.mkdir(exist_ok=True, parents=True)
            infiles = property_domains(lang=lang).get_files()
            db = get_wdprop_domain_db(
                dbpath, create_if_missing=True, read_only=False
            ).db
        elif name == "ranges":
            dbpath = Path(output) / "wdprop_ranges.db"
            dbpath.mkdir(exist_ok=True, parents=True)
            infiles = property_ranges(lang=lang).get_files()
            db = get_wdprop_domain_db(
                dbpath, create_if_missing=True, read_only=False
            ).db
        else:
            raise NotImplementedError(name)

        db = load(
            db=db,
            infiles=infiles,
            format=FileFormat.tuple2,
            key_fn=str.encode,
            value_fn=orjson.dumps,
            n_processes=8,
            shm_mem_ratio=12,
            shm_mem_limit_mb=128,
        )
        if compact:
            logger.info("Compacting {} db...", name)
            db.compact_range()


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

    db = load(
        db=get_wp2wd_db(dbpath, create_if_missing=True, read_only=False).db,
        infiles=wp2wd(lang=lang).get_files(),
        format=FileFormat.tuple2,
        key_fn=str.encode,
        value_fn=str.encode,
        n_processes=8,
        shm_mem_ratio=12,
        shm_mem_limit_mb=128,
    )
    if compact:
        logger.info("Run compaction...")
        db.compact_range()


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
wikidata.add_command(db_entity_labels)
wikidata.add_command(db_entity_redirections)
wikidata.add_command(db_classes)
wikidata.add_command(db_properties)
wikidata.add_command(db_wp2wd)
wikidata.add_command(search_entities)
wikidata.add_command(search_props)
wikidata.add_command(search_classes)


if __name__ == "__main__":
    wikidata()
