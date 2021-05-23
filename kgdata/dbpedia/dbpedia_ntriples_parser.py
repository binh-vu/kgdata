import os
from .dbpediamodels import *
from pathlib import Path
from tqdm.auto import tqdm
import gzip
from dataclasses import asdict
from kgdata.misc import NTriplesParser, get_open_fn
from kgdata.misc.dbpedia.table_tests import *
from kgdata.misc.dbpedia.table_extraction import *
from cityhash import CityHash64


"""
This module provides functions to work with DBPedia raw html dumps
"""


def get_bucket_key(triple: bytes):
    s = orjson.loads(triple)[0]
    return CityHash64(s)


def get_key_pair(triple: bytes):
    x = orjson.loads(triple)
    return x[0], x


def extract_predicate(triple, accum):
    accum.add(triple[1])
    return accum


def set_dumps(row):
    return orjson.dumps(list(row))


def reformat_ntriples_file(infile: str, outdir: str, report: bool=False):
    """Reformat an N-triples file to a json file
    
    Parameters
    ----------
    infile : str
        input n-triples file
    outdir : str
        output directory, filename will be the same as the infile
    report : bool, optional
        report the progress, by default False
    """
    assert os.path.exists(outdir), f"Output directory {outdir} does not exist"
    outfile = os.path.join(outdir, Path(infile).name)

    with get_open_fn(infile)(infile, "rb") as f, \
        gzip.open(outfile, "wb") as g:
        for line in (tqdm(f, desc=f"reformat file: {Path(infile).name}") if report else f):
            parser = NTriplesParser()
            s, p, o = parser.parseline(line.decode())
            s, p, o = str(s), str(p), str(o)

            # g.write(ujson.dumps([s, p, o]).encode())
            g.write(orjson.dumps([s, p, o]))
            g.write(b"\n")

