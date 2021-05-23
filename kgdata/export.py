import glob
import os
from typing import Optional

import rocksdb
from tqdm import tqdm

from kgdata.config import WIKIDATA_DIR
from kgdata.misc import deserialize_key_val_lines, deserialize_jl

"""Export datasets to your databases.
"""


def export_keyvalue_to_rocksdb(indir: str, outdir: Optional[str] = None, is_jl: bool = True, delimiter="\t"):
    """Export key value files to rocksdb"""
    outdir = outdir if outdir is not None else str(indir) + ".db"

    db = rocksdb.DB(outdir, rocksdb.Options(create_if_missing=True))

    for infile in tqdm(glob.glob(os.path.join(indir, "*.gz"))):
        if is_jl:
            rows = deserialize_jl(infile)
        else:
            rows = deserialize_key_val_lines(infile, delimiter=delimiter)

        wb = rocksdb.WriteBatch()
        for id, item in rows:
            wb.put(id.encode(), item.encode())
        db.write(wb)


if __name__ == '__main__':
    export_keyvalue_to_rocksdb(WIKIDATA_DIR + "/step_2/enwiki_links")
