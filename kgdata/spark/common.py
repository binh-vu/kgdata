"""Utility functions for Apache Spark."""

from __future__ import annotations

import hashlib
import math
import os
import random
import shutil
from dataclasses import dataclass
from operator import add, itemgetter
from pathlib import Path
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import orjson
import zstandard as zstd
from kgdata.misc.funcs import deser_zstd_records
from loguru import logger
from pyspark import RDD, SparkConf, SparkContext, TaskContext
from sm.misc.funcs import assert_not_null

# SparkContext singleton
_sc = None
StrPath = Union[Path, str]

R1 = TypeVar("R1")
R2 = TypeVar("R2")
R3 = TypeVar("R3")
K1 = TypeVar("K1")
K2 = TypeVar("K2")
K = TypeVar("K")
V = TypeVar("V")
V2 = TypeVar("V2")


def get_spark_context() -> SparkContext:
    """Get spark context

    Returns
    -------
    SparkContext
    """
    global _sc
    if _sc is None:

        def has_key(key):
            return any(
                k in os.environ
                for k in [
                    key,
                    key.upper(),
                    key.replace(".", "_"),
                    key.upper().replace(".", "_"),
                ]
            )

        def get_key(key):
            lst = [
                os.environ[k]
                for k in [
                    key,
                    key.upper(),
                    key.replace(".", "_"),
                    key.upper().replace(".", "_"),
                ]
                if k in os.environ
            ]
            assert len(lst) > 0, f"{key}: {has_key(key)}"
            return lst[0]

        opts = [
            (key, get_key(key))
            for key in [
                "spark.master",
                "spark.ui.port",
                "spark.executor.memory",
                "spark.executor.cores",
                "spark.executor.instances",
                "spark.driver.memory",
                "spark.driver.maxResultSize",
                "spark.driver.extraLibraryPath",
                "spark.executor.extraLibraryPath",
            ]
            if has_key(key)
        ]
        logger.debug("Spark Options: {}", opts)
        conf = SparkConf().setAll(opts)
        _sc = SparkContext(conf=conf)

        # add the current package, run `python setup.py bdist_egg` if does not exist
        dist_dir = Path(os.path.abspath(__file__)).parent.parent / "dist"
        if dist_dir.exists():
            egg_file = [
                str(file)
                for file in (dist_dir).iterdir()
                if file.name.endswith(".egg") or file.name.endswith(".zip")
            ]
            if len(egg_file) > 0:
                assert len(egg_file) == 1, f"{len(egg_file)} != 1"
                egg_file = egg_file[0]
                _sc.addPyFile(egg_file)

    return _sc


def close_spark_context():
    global _sc
    if _sc is not None:
        _sc.stop()
        _sc = None


def does_result_dir_exist(
    dpath: Union[str, Path],
    allow_override: bool = True,
    create_if_not_exist: bool = False,
) -> bool:
    """Check if the result directory exists

    Args:
        dpath (Union[str, Path]): path to the result directory
        allow_override (bool, optional): allow override the result directory if the result is not success. Defaults to True.
    """
    dpath = str(dpath)
    if not os.path.exists(dpath):
        if create_if_not_exist:
            Path(dpath).mkdir(parents=True)
        return False
    if not os.path.exists(os.path.join(dpath, "_SUCCESS")):
        if allow_override:
            shutil.rmtree(dpath)
            if create_if_not_exist:
                Path(dpath).mkdir(parents=True)
            return False
        raise Exception(
            "Result directory exists. However, it is not a successful attempt."
        )
    return True


def are_records_unique(
    rdd: RDD[R1],
    keyfn: Callable[[R1], Union[str, int]],
    print_error: bool = True,
    return_duplication: bool = False,
):
    """Whether RDDs contain unique records

    Parameters
    ----------
    rdd : RDD
        input dataset
    keyfn : Callable[[Any], Union[int, str]]
        function that get key value of a record
    """
    a = rdd.count()
    b = rdd.map(keyfn).distinct().count()

    if a != b:
        if not print_error:
            raise Exception(
                f"There are {b - a} duplicated records on total {a} records"
            )
        # take first 20 duplicated examples for debugging
        dup_record_ids = (
            rdd.map(lambda x: (keyfn(x), 1))
            .reduceByKey(add)
            .filter(lambda x: x[1] > 1)
            .map(itemgetter(0))
            .take(20)
        )
        dup_record_ids = set(dup_record_ids)

        dup_records = rdd.filter(lambda x: keyfn(x) in dup_record_ids).collect()
        print("Found duplicated records:")
        for r in dup_records:
            print(">>", keyfn(r))
        if return_duplication:
            return dup_records
        return False
    return True


def left_outer_join_repartition(
    rdd1: RDD[Tuple[K, V]],
    rdd2: RDD[Tuple[K, V2]],
    threshold: int = 10000,
    batch_size: int = 1000,
    num_partitions: Optional[int] = None,
):
    """This join is useful in the following scenario:

    1. rdd1 contains duplicated keys, and potentially high cardinality keys
    2. rdd2 contains **unique** keys

    To avoid high cardinality keys, we artificially generate new keys that have the following format (key, category)
    where category is a number between [1, n], then perform the join.
    """
    # finding the keys that have high cardinality
    sc = get_spark_context()

    keys_freq = (
        rdd1.map(lambda x: (x[0], 1))
        .reduceByKey(add)
        .filter(lambda x: x[1] > threshold)
        .collect()
    )
    logger.info("Number of keys with high cardinality: {}", len(keys_freq))
    keys_freq = sc.broadcast(dict(keys_freq))

    def rdd1_gen_key(value: Tuple[K, V]) -> Tuple[Tuple[K, int], V]:
        key = value[0]
        if key not in keys_freq.value:
            return (key, 0), value[1]
        freq = keys_freq.value[key]
        n = math.ceil(freq / batch_size)
        return (key, random.randint(1, n)), value[1]

    def rdd2_gen_key(value: Tuple[K, V2]) -> List[Tuple[Tuple[K, int], V2]]:
        key = value[0]
        if key not in keys_freq.value:
            return [((key, 0), value[1])]
        freq = keys_freq.value[key]
        n = math.ceil(freq / batch_size)
        return [((key, i), value[1]) for i in range(1, n + 1)]

    return (
        rdd1.map(rdd1_gen_key)
        .groupByKey(numPartitions=num_partitions)
        .leftOuterJoin(rdd2.flatMap(rdd2_gen_key))
        .map(lambda x: (x[0][0], x[1]))
    )


def join_repartition(
    rdd1: RDD[Tuple[K, V]],
    rdd2: RDD[Tuple[K, V2]],
    threshold: int = 10000,
    batch_size: int = 1000,
    num_partitions: Optional[int] = None,
):
    """This join is useful in the following scenario:

    1. rdd1 contains duplicated keys, and potentially high cardinality keys
    2. rdd2 contains **unique** keys

    To avoid high cardinality keys, we artificially generate new keys that have the following format (key, category)
    where category is a number between [1, n], then perform the join.
    """
    # finding the keys that have high cardinality
    sc = get_spark_context()

    keys_freq = (
        rdd1.map(lambda x: (x[0], 1))
        .reduceByKey(add)
        .filter(lambda x: x[1] > threshold)
        .collect()
    )
    logger.info("Number of keys with high cardinality: {}", len(keys_freq))
    keys_freq = sc.broadcast(dict(keys_freq))

    def rdd1_gen_key(value: Tuple[K, V]) -> Tuple[Tuple[K, int], V]:
        key = value[0]
        if key not in keys_freq.value:
            return (key, 0), value[1]
        freq = keys_freq.value[key]
        n = math.ceil(freq / batch_size)
        return (key, random.randint(1, n)), value[1]

    def rdd2_gen_key(value: Tuple[K, V2]) -> List[Tuple[Tuple[K, int], V2]]:
        key = value[0]
        if key not in keys_freq.value:
            return [((key, 0), value[1])]
        freq = keys_freq.value[key]
        n = math.ceil(freq / batch_size)
        return [((key, i), value[1]) for i in range(1, n + 1)]

    return (
        rdd1.map(rdd1_gen_key)
        .groupByKey(numPartitions=num_partitions)
        .join(rdd2.flatMap(rdd2_gen_key))
        .map(lambda x: (x[0][0], x[1]))
    )


def left_outer_join(
    rdd1: RDD[R1],
    rdd2: RDD[R2],
    rdd1_keyfn: Callable[[R1], K1],
    rdd1_fk_fn: Callable[[R1], Sequence[K2]],
    rdd2_keyfn: Callable[[R2], K2],
    join_fn: Callable[[R1, List[Tuple[K2, Optional[R2]]]], R3],
    ser_fn: Optional[Callable[[R3], Union[str, bytes]]] = None,
    outfile: Optional[str] = None,
    compression: bool = True,
) -> RDD[R3]:
    """Join two RDDs (left outer join) by non primary key in RDD1.

    RDD1: contains records of (x, Y, x_data) where x is the id of the record, Y are list of ids of records in RDD2.
    RDD2: contains records of (y, y_data) where y is the id of the record.

    Parameters
    ----------
    rdd1 : RDD[R1]
        records of (x, Y, x_data) where x is the id of the record, Y are list of ids of records in RDD2.
    rdd2 : RDD[R2]
        records of (y, y_data) where y is the id of the record.
    rdd1_keyfn : Callable[[R1], K1]
        function that extract id of a record (x) of RDD1
    rdd1_fk_fn : Callable[[R1], List[K2]]
        function that extract Y from a record of RDD1
    rdd2_keyfn : Callable[[R2], K2]
        function that extract id of a record (y) of RDD2
    rdd1_join_fn : Callable[[R1, List[Tuple[K2, Optional[R2]]]], R3]
        function that merge list of Y into record R1
    rdd1_serfn : Optional[Callable[[R3], Union[str, bytes]]]
        function that serialize records of RDD1 to save to file
    outfile : Optional[str]
        output file -- save the result to file if request
    compression : bool, optional
        whether we should compress the result, by default True

    Returns
    -------
    RDD[R3] the merged records
    """
    sc = get_spark_context()

    def p_1_swap_keys(r1: R1):
        x = rdd1_keyfn(r1)
        return [(y, x) for y in rdd1_fk_fn(r1)]

    def p_2_process_join(x):
        # record2 can be nullable if y doesn't exist in RDD2
        y, (record1_ids, record2) = x
        return [(rid, (y, record2)) for rid in record1_ids]

    def p_3_process_join(x):
        x, (record1, record2_lst_with_ids) = x
        if record2_lst_with_ids is None:
            # this can be none, if a record doesn't have any foreign keys
            record2_lst_with_ids = []
        else:
            # convert to list because group by key return resultiterable
            record2_lst_with_ids = list(record2_lst_with_ids)
        return join_fn(record1, record2_lst_with_ids)

    # converts to (y => record2)
    rdd2_v2: RDD[tuple[K2, R2]] = rdd2.map(lambda x: (rdd2_keyfn(x), x))
    # get (y => List[record1 ids])
    rdd3: RDD[tuple[K2, Iterable[K1]]] = rdd1.flatMap(p_1_swap_keys).groupByKey()
    # join with rdd2 and swap the key, to get: (xid => List[(y, record2)])
    rdd4: RDD[tuple[K1, Iterable[tuple[K2, R2]]]] = (
        rdd3.leftOuterJoin(rdd2_v2).flatMap(p_2_process_join).groupByKey()
    )
    # join with rdd1 to merge and join the result
    rdd1_v2: RDD[tuple[K1, R1]] = rdd1.map(lambda x: (rdd1_keyfn(x), x))
    rdd1_v3 = rdd1_v2.leftOuterJoin(rdd4).map(p_3_process_join)

    if outfile is not None:
        assert ser_fn is not None
        if compression:
            rdd1_v3.map(ser_fn).saveAsTextFile(
                outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
            )
        else:
            rdd1_v3.map(ser_fn).saveAsTextFile(outfile)

    return rdd1_v3


def left_outer_join_broadcast(
    rdd1,
    rdd2,
    rdd1_fk_fn: Callable[[R1], List[K2]],
    rdd2_keyfn: Callable[[R2], K2],
    rdd1_join_fn: Callable[[R1, List[Tuple[K2, Optional[R2]]]], None],
    rdd1_serfn: Callable[[R1], Union[str, bytes]],
    outfile: str,
    compression: bool = True,
):
    """Join two RDDs (left outer join) by non primary key in RDD1. This join assumes that RDD2 can fit in memory, and takes the broadcast approach.

    RDD1: contains records of (x, Y, x_data) where x is the id of the record, Y are list of ids of records in RDD2.
    RDD2: contains records of (y, y_data) where y is the id of the record.

    Parameters
    ----------
    rdd1 : RDD[R1]
        records of (x, Y, x_data) where x is the id of the record, Y are list of ids of records in RDD2.
    rdd2 : RDD[R2]
        records of (y, y_data) where y is the id of the record.
    rdd1_fk_fn : Callable[[R1], List[K2]]
        function that extract Y from a record of RDD1
    rdd2_keyfn : Callable[[R2], K2]
        function that extract id of a record (y) of RDD2
    rdd1_join_fn : Callable[[R1, List[Tuple[K2, Optional[R2]]]], None]
        function that merge list of Y into record R1
    rdd1_serfn : Callable[[R1], Union[str, bytes]]
        function that serialize records of RDD1 to save to file
    outfile : str
        output file
    compression : bool, optional
        whether we should compress the result, by default True
    """
    sc = get_spark_context()

    rdd2_val = dict(rdd2.map(lambda x: (rdd2_keyfn(x), x)).collect())
    rdd2_val = sc.broadcast(rdd2_val)

    def join_with_rdd2(record1: R1):
        Y = rdd1_fk_fn(record1)
        result = []
        for y in Y:
            record2 = rdd2_val.value.get(y, None)
            result.append((y, record2))

        rdd1_join_fn(record1, result)
        return record1

    rdd1 = rdd1.map(join_with_rdd2).map(rdd1_serfn)

    if compression:
        rdd1.saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )
    else:
        rdd1.saveAsTextFile(outfile)


def head(rdd, n: int):
    sc = get_spark_context()
    return sc.parallelize(rdd.take(n))


def cache_rdd(rdd, outfile, serfn: Callable[[Any], str], deserfn: Callable[[str], Any]):
    if not does_result_dir_exist(outfile):
        rdd.map(serfn).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    return get_spark_context().textFile(outfile).map(deserfn)


def fix_rdd():
    # template to fix an rdd
    # ###############################
    # TODO: set input file
    indir = "/nas/home/binhvu/workspace/sm-dev/data/wikidata/step_2/schema"
    infile = indir + "/class_schema"

    # ###############################
    newfile = infile + "_new"
    rdd = get_spark_context().textFile(infile)

    # ###############################
    # TODO: update the rdd & serialize it
    rdd = rdd.map(orjson.loads).map(lambda x: x[0] + "\t" + orjson.dumps(x[1]).decode())
    # rdd = rdd.map(orjson.dumps)
    # ###############################

    rdd.saveAsTextFile(
        newfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
    )
    os.rename(infile, infile + "_old")
    os.rename(newfile, infile)


def estimate_num_partitions(rdd: RDD[str] | RDD[bytes], partition_size: int) -> int:
    """Estimate the number of partitions needed to store the RDD with the given partition size (bytes)."""

    def get_bytes(s: str | bytes) -> int:
        return len(s.encode()) if isinstance(s, str) else len(s)

    total_size = rdd.map(get_bytes).sum()
    return math.ceil(total_size / partition_size)


def save_as_text_file(
    rdd: RDD[str] | RDD[bytes],
    outdir: Path,
    compression: Optional[Literal["gz", "zst"]],
    compression_level: Optional[int] = None,
):
    if compression == "gz" or compression is None:
        return rdd.saveAsTextFile(
            str(outdir),
            compressionCodecClass=(
                "org.apache.hadoop.io.compress.GzipCodec"
                if compression == "gz"
                else None
            ),
        )

    if compression == "zst":
        compression_level = compression_level or 3

        def save_partition(partition: Iterable[str] | Iterable[bytes]):
            partition_id = assert_not_null(TaskContext.get()).partitionId()
            it = iter(partition)
            first_val = next(it, None)
            if first_val is None:
                # empty partition
                return

            lst = []

            if isinstance(first_val, str):
                first_val = first_val.encode()
                if not first_val.endswith(b"\n"):
                    lst.append(first_val + b"\n")
                else:
                    lst.append(first_val)

                for x in it:
                    x = x.encode()  # type: ignore
                    if not x.endswith(b"\n"):
                        x = x + b"\n"
                    lst.append(x)
            else:
                if not first_val.endswith(b"\n"):
                    lst.append(first_val + b"\n")
                else:
                    lst.append(first_val)
                for x in it:
                    if not x.endswith(b"\n"):
                        x = x + b"\n"
                    lst.append(x)

            lst[-1] = lst[-1][:-1]  # exclude last \n

            datasize = sum(len(x) for x in lst)
            cctx = zstd.ZstdCompressor(level=compression_level, write_content_size=True)

            with open(outdir / f"part-{partition_id:05d}.zst", "wb") as fh:
                with cctx.stream_writer(fh, size=datasize) as f:
                    for x in lst:
                        f.write(x)

        outdir.mkdir(parents=True, exist_ok=True)
        rdd.foreachPartition(save_partition)
        (outdir / "_SUCCESS").touch()
        return

    raise Exception(f"Unknown compression: {compression}")


def text_file(
    filepattern: StrPath, min_partitions: Optional[int] = None, use_unicode: bool = True
) -> RDD[str]:
    """Drop-in replacement for SparkContext.textFile that supports zstd files."""
    filepattern = Path(filepattern)
    # to support zst files (indir)
    if (
        filepattern.is_dir()
        and any(
            file.name.startswith("part-") and file.name.endswith(".zst")
            for file in filepattern.iterdir()
        )
    ) or filepattern.name.endswith(".zst"):
        if filepattern.is_dir():
            n_parts = sum(
                1 for file in filepattern.iterdir() if file.name.startswith("part-")
            )
        else:
            n_parts = sum(1 for _ in filepattern.parent.glob("*.zst"))

        return (
            get_spark_context()
            .binaryFiles(str(filepattern))
            .repartition(n_parts)
            .flatMap(
                lambda x: [x.decode() for x in deser_zstd_records(x[1])],
                preservesPartitioning=True,
            )
        )

    return get_spark_context().textFile(str(filepattern), min_partitions, use_unicode)


def diff_rdd(rdd1: RDD[str], rdd2: RDD[str], key: Callable[[str], str]):
    """Compare content of two RDDs

    Parameters
    ----------
    rdd1 : RDD[str]
        first RDD
    rdd2 : RDD[str]
        second RDD
    key : Callable[[str], str]
        function that extract key from a record

    Returns
    -------
    RDD[str]
        records that are in rdd1 but not in rdd2
    """

    def convert(x):
        k = key(x)
        if not isinstance(x, bytes):
            x = x.encode()
        return k, hashlib.sha256(x).digest().hex()

    max_size = 100
    records = (
        rdd1.map(convert)
        .fullOuterJoin(rdd2.map(convert))
        .filter(lambda x: x[1][0] != x[1][1])
        .take(max_size)
    )
    if len(records) == 0:
        print("No difference")
        return
    print(
        f"Found {'at least' if len(records) >= max_size else ''} {len(records)} difference:"
    )
    for r in records:
        print(r[0], r[1][0], r[1][1])


@dataclass
class EmptyBroadcast(Generic[V]):
    value: V
    value: V
    value: V
