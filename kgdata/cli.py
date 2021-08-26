import os
from enum import Enum
from pathlib import Path

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
    "-c", "--compression", default=False, help="Whether to compress the results"
)
def wikidata(build: str, directory: str, output_dir: str, compression: bool):
    try:
        build = WDBuildOption(build)
    except ValueError:
        logger.error("Invalid build value: {}. Exiting!", build)
        return

    directory = directory.strip()
    output_dir = Path(output_dir.strip())
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
    from kgdata.spark import rdd2db

    logger.info("Wikidata directory: {}", WIKIDATA_DIR)
    logger.info("Build: {}", build)

    # process the raw wikidata dump
    prep01(overwrite=False)
    # extract qnodes from wikidata english
    qnode_files = os.path.join(WIKIDATA_DIR, "step_2/qnodes_en")
    qnodes_en(outfile=qnode_files)

    if build == WDBuildOption.Qnodes:
        rdd2db(
            os.path.join(qnode_files, "*.gz"),
            os.path.join(output_dir, "qnodes.db"),
            format="jsonline",
            compression=compression,
            verbose=True,
        )

    if build in [WDBuildOption.WDProps, WDBuildOption.WDClasses]:
        make_ontology()
        make_superclass_closure()
        # TODO: uncomment to verify if the data is correct
        # examine_ontology_property()

        if build == WDBuildOption.WDProps:
            save_wdprops(output_dir)

        if build == WDBuildOption.WDClasses:
            save_wdclasses(output_dir)


@click.group()
def cli():
    pass


cli.add_command(wikidata)

if __name__ == "__main__":
    cli()
