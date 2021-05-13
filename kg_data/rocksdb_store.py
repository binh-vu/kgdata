import glob

import rocksdb
from tqdm.auto import tqdm

import sm_unk.misc as M


def convert_rdd_to_level_db(rdd_dir: str, is_jl: bool = True):
    db = rocksdb.DB(rdd_dir + ".db", rocksdb.Options(create_if_missing=True))

    for infile in tqdm(glob.glob(str(rdd_dir) + "/*.gz")):
        if is_jl:
            rows = M.deserialize_jl(infile)
        else:
            rows = M.deserialize_key_val_lines(infile)

        wb = rocksdb.WriteBatch()
        for id, item in rows:
            wb.put(id.encode(), item.encode())
        db.write(wb)


if __name__ == '__main__':
    # convert_rdd_to_level_db("/workspace/sm-dev/data/wikidata/step_2/enwiki_links")
    convert_rdd_to_level_db("/workspace/sm-dev/data/wikidata/step_2/class_schema", is_jl=False)