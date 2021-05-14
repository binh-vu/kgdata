import os, orjson, glob, codecs
import pickle
import shutil
from pathlib import Path
from pyspark import SparkContext, SparkConf
from operator import add, itemgetter
from typing import Any, TypeVar, Callable, List, Union, Tuple, Optional

# SparkContext singleton
_sc = None


def get_spark_context():
    """Get spark context

    Returns
    -------
    SparkContext
    """
    global _sc
    if _sc is None:
        conf = (
            SparkConf()
            .setMaster(os.environ["SPARK_MASTER"])
            .setAll(
                [
                    ("spark.executor.memory", os.environ["SPARK_EXECUTOR_MEM"]),
                    ("spark.executor.cores", os.environ["SPARK_EXECUTOR_CORES"]),
                    ("spark.executor.instances", os.environ["SPARK_NUM_EXECUTORS"]),
                    ("spark.ui.port", int(os.environ["SPARK_UI_PORT"])),
                    ("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "1g")),
                    (
                        "spark.driver.maxResultSize",
                        os.environ.get("SPARK_DRIVER_RESULTSIZE", "10g"),
                    ),
                ]
            )
        )
        _sc = SparkContext(conf=conf)

        # add the current package, run `python setup.py bdist_egg` if does not exist
        egg_file = [
            str(file)
            for file in (
                Path(os.path.abspath(__file__)).parent.parent / "dist"
            ).iterdir()
            if file.name.endswith(".egg") or file.name.endswith(".whl")
        ]
        assert len(egg_file) == 1, f"{len(egg_file)} != 1"
        egg_file = egg_file[0]

        _sc.addPyFile(egg_file)

    return _sc


def close_spark_context():
    global _sc
    if _sc is not None:
        _sc.stop()
        _sc = None


def does_result_dir_exist(dpath, allow_override=True):
    if not os.path.exists(dpath):
        return False
    if not os.path.exists(os.path.join(dpath, "_SUCCESS")):
        if allow_override:
            shutil.rmtree(dpath)
            return False
        raise Exception(
            "Result directory exists. However, it is not a successful attempt."
        )
    return True


def ensure_unique_records(rdd, keyfn, print_error: bool = True):
    """Make sure that RDDs contain unique records

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
        for r in dup_records:
            print(">>", r)
        return False
    return True


R1 = TypeVar("R1")
R2 = TypeVar("R2")
K1 = TypeVar("K1")
K2 = TypeVar("K2")


def left_outer_join(
    rdd1,
    rdd2,
    rdd1_keyfn: Callable[[R1], K1],
    rdd1_fk_fn: Callable[[R1], List[K2]],
    rdd2_keyfn: Callable[[R2], K2],
    join_fn: Callable[[R1, List[Tuple[K2, Optional[R2]]]], Optional[R1]],
    ser_fn: Callable[[R1], Union[str, bytes]],
    outfile: str,
    compression: bool = True,
):
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
    rdd1_join_fn : Callable[[R1, List[Tuple[K2, Optional[R2]]]], Optional[None]]
        function that merge list of Y into record R1, if its return not None, we use that value
    rdd1_serfn : Callable[[R1], Union[str, bytes]]
        function that serialize records of RDD1 to save to file
    outfile : str
        output file
    compression : bool, optional
        whether we should compress the result, by default True
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
        resp = join_fn(record1, record2_lst_with_ids)
        if resp is not None:
            return resp
        return record1

    # converts to (y => record2)
    rdd2 = rdd2.map(lambda x: (rdd2_keyfn(x), x))
    # get (y => List[record1 ids])
    rdd3 = rdd1.flatMap(p_1_swap_keys).groupByKey()
    # join with rdd2 and swap the key, to get: (xid => List[(y, record2)])
    rdd4 = rdd3.leftOuterJoin(rdd2).flatMap(p_2_process_join).groupByKey()
    # join with rdd1 to merge and join the result
    rdd1 = rdd1.map(lambda x: (rdd1_keyfn(x), x))
    rdd1 = rdd1.leftOuterJoin(rdd4).map(p_3_process_join).map(ser_fn)

    if compression:
        rdd1.saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )
    else:
        rdd1.saveAsTextFile(outfile)


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


def saveAsSingleTextFile(rdd, outfile, compressionCodecClass=None):
    rdd = rdd.coalesce(1, shuffle=True)
    if os.path.exists(outfile + "_tmp"):
        shutil.rmtree(outfile + "_tmp")
        
    if compressionCodecClass is not None:
        rdd.saveAsTextFile(
            outfile + "_tmp", compressionCodecClass=compressionCodecClass
        )
    else:
        rdd.saveAsTextFile(outfile + "_tmp")
    shutil.move(glob.glob(os.path.join(outfile + "_tmp", "part-00000*"))[0], outfile)
    shutil.rmtree(outfile + "_tmp")


def cache_rdd(rdd, outfile, serfn: Callable[[Any], str], deserfn: Callable[[str], Any]):
    if not does_result_dir_exist(outfile):
        rdd \
            .map(serfn) \
            .saveAsTextFile(outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
    
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

    rdd.saveAsTextFile(newfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
    os.rename(infile, infile + "_old")
    os.rename(newfile, infile)


if __name__ == '__main__':
    # fix RDD
    fix_rdd()