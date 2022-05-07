import glob
import os
from enum import Enum
from operator import itemgetter
from pathlib import Path
from typing import Literal, get_args

import click
import orjson
import ujson
from hugedict.loader import FileFormat, load, load_single_file
from hugedict.misc import Chain2, identity, zstd6_compress
from loguru import logger


Dataset = Literal["enterprise_html_dumps"]
valid_datasets = get_args(Dataset)


@click.command()
@click.argument(
    "dataset",
    type=click.Choice(valid_datasets, case_sensitive=False),
)
@click.option("-d", "--directory", default="", help="Wikipedia directory")
@click.option(
    "-c",
    "--compact",
    is_flag=True,
    help="Whether to compact the results. May take a very very long time",
)
def wikipedia(dataset: Dataset, directory: str, compact: bool):
    """Build Wikipedia datasets from Wikipedia dumps."""
    directory = directory.strip()

    if directory != "":
        os.environ["WIKIPEDIA_DIR"] = directory

    from kgdata.config import WIKIPEDIA_DIR
    from kgdata.wikipedia.datasets.html_pages import enterprise_html_dumps

    if dataset == "enterprise_html_dumps":
        enterprise_html_dumps()
        return
