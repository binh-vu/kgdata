import os
from enum import Enum
from pathlib import Path
from typing import Literal

import click
from loguru import logger


class WDBuildOption(str, Enum):
    Qnodes = "qnodes"
    WDClasses = "wdclasses"
    WDProps = "wdprops"


@click.command()
@click.option("-b", "--build", help="Build database")
@click.option("-d", "--directory", default="", help="Wikidata directory")
@click.option("-o", "--output", help="Output directory")
@click.option(
    "-c", "--compression", is_flag=True, help="Whether to compress the results"
)
def wikidata(build: Literal["qnodes", "wdclasses", "wdprops"], directory: str, output: str, compression: bool):
    try:
        assert build in ["qnodes", "wdclasses", "wdprops"]
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
    from kgdata.wikidata.rdd_datasets import qnodes_en
    from kgdata.wikidata.ontology import (
        save_wdprops,
        save_wdclasses,
        make_ontology,
        make_superclass_closure,
        examine_ontology_property,
    )
    from kgdata.spark import rdd2db, does_result_dir_exist

    logger.info("Wikidata directory: {}", WIKIDATA_DIR)
    logger.info("Build: {}", build)

    # process the raw wikidata dump
    prep01(overwrite=False)
    # extract qnodes from wikidata english
    qnode_files = os.path.join(WIKIDATA_DIR, "step_2/qnodes_en")
    if not does_result_dir_exist(qnode_files):
        qnodes_en(outfile=qnode_files)

    if build == "qnodes":
        rdd2db(
            os.path.join(qnode_files, "*.gz"),
            os.path.join(output_dir, "qnodes.db"),
            format="jsonline",
            compression=compression,
            verbose=True,
        )

    if build in ["wdclasses", "wdprops"]:
        make_ontology()
        make_superclass_closure()
        # TODO: uncomment to verify if the data is correct
        # examine_ontology_property()

        if build == WDBuildOption.WDProps:
            save_wdprops(
                indir=os.path.join(WIKIDATA_DIR, "ontology"), 
                outdir=output_dir
            )

        if build == WDBuildOption.WDClasses:
            save_wdclasses(
                indir=os.path.join(WIKIDATA_DIR, "ontology"), 
                outdir=output_dir
            )
            logger.info("Finish saving wdclasses to DB")


@click.group()
def cli():
    pass


cli.add_command(wikidata)

if __name__ == "__main__":
    cli()
