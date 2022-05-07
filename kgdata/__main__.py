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
from kgdata.wikidata.cli import wikidata
from kgdata.wikipedia.cli import wikipedia


@click.group()
def cli():
    pass


cli.add_command(wikidata)
cli.add_command(wikipedia)


if __name__ == "__main__":
    cli()
