# %%

import hashlib
from pathlib import Path

import orjson

from kgdata.spark.common import diff_rdd, get_spark_context, text_file

files1 = "/var/tmp/kgdata/wikidata/20230619/024_entity_types/*.gz"
files2 = "/data/binhvu/data/wikidata/20230619/024_entity_types/*.gz"


# %%


def norm(x):
    a, ts = orjson.loads(x)
    return orjson.dumps([a, sorted(ts)])


rdd1 = text_file(files1).map(norm)
rdd2 = text_file(files2).map(norm)

diff_rdd(rdd1, rdd2, lambda x: orjson.loads(x)[0])
# %%

val1 = rdd1.map(orjson.loads).filter(lambda x: x[0] == "Q98130237").take(1)
val2 = rdd2.map(orjson.loads).filter(lambda x: x[0] == "Q98130237").take(1)

# %%


def convert(x):
    key = orjson.loads(x)["id"]
    return key, x


def hash(self):
    """Hash the RDD. To get a commutative hash, we use add function with little worry about hashing items to zero.

    Reference: https://kevinventullo.com/2018/12/24/hashing-unordered-sets-how-far-will-cleverness-take-you/
    """

    zero = (0).to_bytes(32, byteorder="little")
    maxint = (2**256 - 1).to_bytes(32, byteorder="little")

    def hash(line: str | bytes):
        if isinstance(line, str):
            line = line.encode()

        return hashlib.sha256(line).digest()

    def sum_hash(hash1: bytes, hash2: bytes):
        val = int.from_bytes(hash1, byteorder="little", signed=False) + int.from_bytes(
            hash2, byteorder="little", signed=False
        )
        return (
            val % int.from_bytes(maxint, byteorder="little", signed=False)
        ).to_bytes(32, byteorder="little")

    return self.map(hash).fold(zero, sum_hash)


# print(text_file(files2).count())
# %%
filepattern = Path(files1)
if filepattern.is_dir():
    n_parts = sum(1 for file in filepattern.iterdir() if file.name.startswith("part-"))
else:
    n_parts = sum(1 for _ in filepattern.parent.glob("*.zst"))

# %%
text_file(files1).getNumPartitions()

# %%

rdd2 = text_file(files2).map(convert)
rdd1 = text_file(files1).map(convert)

lst1 = rdd1.take(1000)
lst2 = rdd2.take(1000)

# %%

dict1 = dict(lst1)
dict2 = dict(lst2)
# %%

set(dict1.keys()).intersection(set(dict2.keys()))

# %%
