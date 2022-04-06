import os, glob
from hugedict.misc import Chain2, identity, zstd6_compress
import orjson
import ujson
from enum import Enum
from pathlib import Path
from typing import Literal

import click
from loguru import logger
from operator import itemgetter
from hugedict.loader import load, load_single_file, FileFormat


@click.command()
@click.option("-b", "--build", help="Build database")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
def wikidata(
    build: Literal[
        "qnodes",
        "wdclasses",
        "wdprops",
        "enwiki_links",
        "qnode_labels",
        "qnode_identifiers",
    ],
    directory: str,
    output: str,
    compact: bool,
):
    try:
        assert build in [
            "qnodes",
            "wdclasses",
            "wdprops",
            "enwiki_links",
            "qnode_redirections",
            "qnode_labels",
            "qnode_identifiers",
        ]
    except ValueError:
        logger.error("Invalid build value: {}. Exiting!", build)
        return

    directory = directory.strip()
    output_dir = Path(output.strip())
    output_dir.mkdir(exist_ok=True, parents=True)

    if directory != "":
        os.environ["WIKIDATA_DIR"] = directory

    from kgdata.config import WIKIDATA_DIR
    from kgdata.wikidata.s00_prep_data import prep01
    from kgdata.wikidata.rdd_datasets import (
        qnodes_en,
        wiki_article_to_qnode,
        qnodes_identifier,
    )
    from kgdata.wikidata.ontology import (
        save_wdprops,
        save_wdclasses,
        make_ontology,
        make_superclass_closure,
        examine_ontology_property,
    )
    from kgdata.spark import does_result_dir_exist
    from kgdata.wikidata.db import (
        get_qnode_db,
        get_wdprop_db,
        get_wdclass_db,
        get_qnode_redirection_db,
        get_qnode_label_db,
    )
    from kgdata.wikidata.redirections import extract_redirections

    logger.info("Wikidata directory: {}", WIKIDATA_DIR)
    logger.info("Build: {}. Compaction: {}", build, compact)

    # build qnode redirections do not need entity dumps
    if build == "qnode_redirections":
        redirection_file = extract_redirections(
            page_dump=os.path.join(WIKIDATA_DIR, "step_0/wikidatawiki-page.sql.gz"),
            redirect_dump=os.path.join(
                WIKIDATA_DIR, "step_0/wikidatawiki-redirect.sql.gz"
            ),
            outdir=os.path.join(WIKIDATA_DIR, "redirections"),
        )
        dbpath = os.path.join(output_dir, "qnode_redirections.db")
        db = load_single_file(
            db=get_qnode_redirection_db(
                dbpath, create_if_missing=True, read_only=False
            ).db,
            infile=redirection_file,
            format=FileFormat.tabsep,
            key_fn=identity,
            value_fn=identity,
        )
        if compact:
            logger.info("Run compaction...")
            db.compact_range()
        return

    # process the raw wikidata dump
    prep01(
        indir=os.path.join(WIKIDATA_DIR, "step_0"),
        outdir=os.path.join(WIKIDATA_DIR, "step_1"),
        overwrite=False,
    )
    # extract qnodes from wikidata english
    qnode_files = os.path.join(WIKIDATA_DIR, "step_2/qnodes_en")
    if not does_result_dir_exist(qnode_files):
        qnodes_en(outfile=qnode_files)

    if build == "qnodes":
        dbpath = os.path.join(output_dir, "qnodes.db")
        db = load(
            db=get_qnode_db(dbpath).db,
            infiles=glob.glob(os.path.join(qnode_files, "*.gz")),
            format=FileFormat.jsonline,
            key_fn=Chain2(str.encode, itemgetter("id")).exec,
            value_fn=Chain2(zstd6_compress, orjson.dumps).exec,
            n_processes=8,
            shm_mem_ratio=12,
            shm_mem_limit_mb=128,
        )

        if compact:
            logger.info("Run compaction...")
            db.compact_range()
        return

    if build == "qnode_labels":
        dbpath = os.path.join(output_dir, "qnode_labels.db")
        db = load(
            db=get_qnode_label_db(dbpath, create_if_missing=True, read_only=False).db,
            infiles=glob.glob(os.path.join(qnode_files, "*.gz")),
            format=FileFormat.jsonline,
            key_fn=Chain2(str.encode, itemgetter("id")).exec,
            value_fn=extract_id_label,
            n_processes=8,
            shm_mem_limit_mb=8,
            shm_mem_ratio=20,
        )
        if compact:
            logger.info("Run compaction...")
            db.compact_range()
        return

    if build == "qnode_identifiers":
        qnodes_identifier(outfile=os.path.join(output_dir, "qnode_identifiers.txt"))
        return

    if build == "enwiki_links":
        wiki_article_to_qnode()
        dbpath = os.path.join(output_dir, "enwiki_links.db")
        db = load(
            db=get_qnode_db(dbpath).db,
            infiles=glob.glob(os.path.join(WIKIDATA_DIR, "step_2/enwiki_links/*.gz")),
            format=FileFormat.tuple2,
            key_fn=str.encode,
            value_fn=str.encode,
            n_processes=8,
            shm_mem_limit_mb=8,
            shm_mem_ratio=20,
        )
        if compact:
            logger.info("Run compaction...")
            db.compact_range()
        return

    if build in ["wdclasses", "wdprops"]:
        make_ontology()
        make_superclass_closure()
        # TODO: uncomment to verify if the data is correct
        # examine_ontology_property()
        dbpath = os.path.join(output_dir, f"{build}.db")
        if build == "wdprops":
            db = get_wdprop_db(dbpath, create_if_missing=True, read_only=False).db
            save_wdprops(indir=os.path.join(WIKIDATA_DIR, "ontology"), db=db)
        elif build == "wdclasses":
            db = get_wdclass_db(dbpath, create_if_missing=True, read_only=False).db
            save_wdclasses(indir=os.path.join(WIKIDATA_DIR, "ontology"), db=db)
            logger.info("Finish saving wdclasses to DB")
        else:
            raise Exception("Unreachable!")
        if compact:
            logger.info("Run compaction...")
            db.compact_range()
        return


@click.group()
def cli():
    pass


cli.add_command(wikidata)


def extract_id_label(odict):
    label = odict["label"]
    label = label["lang2value"][label["lang"]]
    return orjson.dumps({"id": odict["id"], "label": label})


if __name__ == "__main__":
    cli()
