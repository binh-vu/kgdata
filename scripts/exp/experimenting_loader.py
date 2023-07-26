"""Experimenting loader using SST"""


import os
from operator import itemgetter
from pathlib import Path
from typing import Any, Callable, List, Optional, Union

import click
import orjson
from hugedict.loader import FileFormat, FileReaderArgs, read_file
from hugedict.misc import Chain2, zstd6_compress
from hugedict.parallel import Parallel
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from loguru import logger
from networkx.readwrite.adjlist import generate_adjlist
from rocksdict import DBCompressionType, Options, Rdict, SstFileWriter
from tqdm import tqdm


def load_wdentities(
    dbpath: Union[str, Path],
    infiles: List[str],
    format: FileFormat,
    key_fn: Callable[[Any], bytes],
    value_fn: Callable[[Any], bytes],
):
    dbpath = Path(dbpath)

    infiles = sorted(infiles)
    pp = Parallel()
    inputs = [
        (
            dbpath / "sst",
            FileReaderArgs(
                infile=Path(infile),
                format=format,
                key_fn=key_fn,
                value_fn=value_fn,
                shm_pool=[],
                shm_reserved=None,  # type: ignore
            ),
        )
        for infile in infiles
    ]

    logger.info("Generating SST files...")
    sst_files = pp.map(
        generate_sst, inputs=inputs, progress_desc="loading", show_progress=True
    )

    logger.info("Loading SST files...")
    opts = Options(raw_mode=True)
    # opts.set_compression_type(DBCompressionType.none())
    db = Rdict(path=str(dbpath / "primary"), options=opts)
    db.ingest_external_file(sst_files)
    db.flush()

    logger.info("Compacting...")
    db.compact_range()
    return db


def generate_sst(outdir: Path, args: FileReaderArgs):
    outfile = outdir / f"{args.infile.stem}.sst"
    outfile.parent.mkdir(parents=True, exist_ok=True)

    if outfile.exists():
        return str(outfile)

    outputs = read_file(args)
    writer = SstFileWriter()
    writer.open(str(outfile))
    for k, v in sorted(outputs, key=itemgetter(0)):
        writer[k] = v
    writer.finish()

    return str(outfile)


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
    """Wikidata entities"""
    WikidataDirCfg.init(directory)

    dbpath = Path(output) / "wdentities.db"
    dbpath.mkdir(exist_ok=True, parents=True)

    load_wdentities(
        dbpath,
        infiles=entities(lang=lang).get_files(),
        format=FileFormat.jsonline,
        key_fn=Chain2(str.encode, itemgetter("id")).exec,
        value_fn=Chain2(zstd6_compress, orjson.dumps).exec,
    )


if __name__ == "__main__":
    db_entities()
