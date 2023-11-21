from __future__ import annotations

import glob
import hashlib
import math
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

import serde.json
from kgdata.spark.common import (
    are_records_unique,
    estimate_num_partitions,
    get_spark_context,
    join_repartition,
    left_outer_join_repartition,
)
from pyspark.rdd import RDD, portable_hash
from typing_extensions import TypeGuard

if TYPE_CHECKING:
    from kgdata.dataset import Dataset


class SupportsOrdering(Protocol):
    def __lt__(self, other: SupportsOrdering) -> bool:
        ...


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
U = TypeVar("U")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
V1 = TypeVar("V1")
V2 = TypeVar("V2")
V3 = TypeVar("V3")


S = TypeVar("S", bound=SupportsOrdering)
StrPath = Union[Path, str]
NEW_DATASET_NAME = "__new__"
NO_CHECKSUM = (b"\x00" * 32).hex()


@dataclass(frozen=True)
class DatasetSignature:
    name: str
    created_at: str
    checksum: str

    dependencies: dict[str, DatasetSignature]

    @staticmethod
    def from_intermediate_dataset(
        sig: DatasetSignature, with_dep: bool = True
    ) -> DatasetSignature:
        deps = {sig.name: sig}
        if with_dep:
            deps.update(sig.dependencies)

        return DatasetSignature(
            name=NEW_DATASET_NAME, created_at="", checksum="", dependencies=deps
        )

    @staticmethod
    def intermediate_dataset(name: str) -> DatasetSignature:
        """Return a signature of an intermediate dataset, which doesn't have any dependencies and checksum isn't important"""
        return DatasetSignature(
            name=name,
            created_at=str(datetime.now().astimezone()),
            checksum=NO_CHECKSUM,
            dependencies={},
        )

    def update(self, **kwargs):
        return DatasetSignature(
            name=kwargs.get("name", self.name),
            created_at=kwargs.get("created_at", self.created_at),
            checksum=kwargs.get("checksum", self.checksum),
            dependencies=kwargs.get("dependencies", self.dependencies),
        )

    def use(self, other: DatasetSignature) -> DatasetSignature:
        """When using another dataset, if this is a new dataset, then the used dataset is added to `self.dependencies`. Otherwise, a new DatasetSignature is created"""
        if self == other:
            return self

        if self.name == NEW_DATASET_NAME:
            newone = DatasetSignature(
                name=self.name,
                created_at=self.created_at,
                checksum=self.checksum,
                dependencies=self.dependencies.copy(),
            )
            if other.name == NEW_DATASET_NAME:
                newone.dependencies.update(other.dependencies)
            else:
                if other.name in newone.dependencies:
                    assert other == newone.dependencies[other.name]
                else:
                    newone.dependencies[other.name] = other
            return newone
        else:
            deps: dict[str, DatasetSignature] = {self.name: self}
            if other.name == NEW_DATASET_NAME:
                deps.update(other.dependencies)
            else:
                deps[other.name] = other
            return DatasetSignature(
                name=NEW_DATASET_NAME,
                created_at="",
                checksum="",
                dependencies=deps,
            )

    def without_dependencies(self) -> DatasetSignature:
        return DatasetSignature(
            name=self.name,
            created_at=self.created_at,
            checksum=self.checksum,
            dependencies={},
        )

    def is_valid(self):
        return (
            self.name != NEW_DATASET_NAME
            and self.created_at != ""
            and self.checksum != ""
            and all(d.is_valid() for d in self.dependencies.values())
        )

    def to_dict(self, no_nested_dep: bool = False):
        return {
            "name": self.name,
            "created_at": self.created_at,
            "checksum": self.checksum,
            "dependencies": sorted(
                [
                    d.without_dependencies().to_dict() if no_nested_dep else d.to_dict()
                    for d in self.dependencies.values()
                ],
                key=lambda d: d["name"],
            ),
        }

    @staticmethod
    def from_dict(obj: dict) -> DatasetSignature:
        return DatasetSignature(
            name=obj["name"],
            created_at=obj["created_at"],
            checksum=obj["checksum"],
            dependencies={
                dd["name"]: DatasetSignature.from_dict(dd) for dd in obj["dependencies"]
            },
        )


class ExtendedRDD(Generic[T_co]):
    """Extended version of RDD providing more utility functions to make it easier to use."""

    def __init__(self, rdd: RDD[T_co], signature: DatasetSignature):
        self.rdd = rdd
        self.sig = signature

    def auto_coalesce(
        self: ExtendedRDD[str] | ExtendedRDD[bytes],
        partition_size: int = 10 * 1024 * 1024,
        cache: bool = False,
        shuffle: bool = False,
    ):
        """Coalesce the RDD so that each partition has approximately the given partition size in bytes.

        The default size is 10MB.
        """
        if cache:
            rdd = self.rdd.cache()
        else:
            rdd = self.rdd

        return ExtendedRDD(
            rdd.coalesce(estimate_num_partitions(rdd, partition_size), shuffle=shuffle),
            self.sig,
        )

    def save_as_single_text_file(
        self, outfile: StrPath, compressionCodecClass: Optional[str] = None
    ):
        rdd = self.rdd.coalesce(1)
        outfile = str(outfile)
        if os.path.exists(outfile + "_tmp"):
            shutil.rmtree(outfile + "_tmp")

        if compressionCodecClass is not None:
            rdd.saveAsTextFile(
                outfile + "_tmp", compressionCodecClass=compressionCodecClass
            )
        else:
            rdd.saveAsTextFile(outfile + "_tmp")
        shutil.move(
            glob.glob(os.path.join(outfile + "_tmp", "part-00000*"))[0], outfile
        )
        shutil.rmtree(outfile + "_tmp")

    def save_like_dataset(
        self,
        dataset: "Dataset",
        checksum: bool = True,
        auto_coalesce: bool = False,
        partition_size: int = 64 * 1024 * 1024,
        shuffle: bool = False,
        min_num_partitions: int = 64,
        max_num_partitions: int = 1024,
        trust_dataset_dependencies: bool = False,
    ) -> None:
        """Save this RDD as a dataset similar to the given dataset. By default, checksum of the dataset is computed
        so we can be confident that the data hasn't changed yet, or multiple copied are indeed equal.

        # Arguments
            dataset: the target dataset to save
            checksum: whether to compute checksum of the dataset. Usually, we don't need to compute the checksum
                for intermediate datasets.
            auto_coalesce: whether to automatically coalesce the RDD so that each partition has approximately the given partition size in bytes.
            partition_size: if auto_coalesce is enable, coalesce the RDD so that each partition has approximately the given partition size in bytes.
            shuffle: if auto_coalesce is enable, whether to shuffle the RDD.
            min_num_partitions: if auto_coalesce is enable and this variable is not None, this will be the minimum number of partitions to coalesce to.
            max_num_partitions: if auto_coalesce is enable and this variable is not None, this will be the maximum number of partitions to coalesce to.
            trust_dataset_dependencies: whether to trust the dataset dependencies. If this is False, we will verify the dataset dependencies and ensure that they are equal.
        """
        file_pattern = Path(dataset.file_pattern)
        if file_pattern.suffix == ".gz":
            compressionCodecClass = "org.apache.hadoop.io.compress.GzipCodec"
        else:
            # this to make sure the dataset file pattern matches the generated file from spark.
            assert file_pattern.suffix == "" and file_pattern.name.startswith(
                "part-"
            ), file_pattern.name
            compressionCodecClass = None

        # verify dataset dependencies
        if trust_dataset_dependencies:
            self.sig = DatasetSignature(
                name=NEW_DATASET_NAME,
                created_at="",
                checksum="",
                dependencies={
                    (s := dep.get_signature()).name: s
                    for dep in dataset.get_dependencies()
                },
            )
        else:
            if self.sig.name != NEW_DATASET_NAME:
                dep_sigs = [self.sig]
            else:
                dep_sigs = sorted(
                    self.sig.dependencies.values(), key=lambda sig: sig.name
                )

            given_dep_sigs = sorted(
                [dep.get_signature() for dep in dataset.get_dependencies()],
                key=lambda sig: sig.name,
            )
            assert dep_sigs == given_dep_sigs, (dep_sigs, given_dep_sigs)

        self.save_as_dataset(
            dataset.get_data_directory(),
            compressionCodecClass=compressionCodecClass,
            name=dataset.name,
            checksum=checksum,
            auto_coalesce=auto_coalesce,
            partition_size=partition_size,
            shuffle=shuffle,
            min_num_partitions=min_num_partitions,
            max_num_partitions=max_num_partitions,
        )

    def save_as_dataset(
        self,
        outdir: StrPath,
        compressionCodecClass: Optional[str] = None,
        name: Optional[str] = None,
        checksum: bool = True,
        auto_coalesce: bool = False,
        partition_size: int = 64 * 1024 * 1024,
        shuffle: bool = False,
        min_num_partitions: int = 64,
        max_num_partitions: int = 1024,
    ):
        """Save this RDD as a dataset. By default, checksum of the dataset is computed
        so we can be confident that the data hasn't changed yet, or multiple copied are indeed equal.

        # Arguments
            outdir: output directory
            compressionCodecClass: compression codec class to use
            name: name of the dataset, by default, we use the output directory name
            checksum: whether to compute checksum of the dataset. Usually, we don't need to compute the checksum
                for intermediate datasets.
            auto_coalesce: whether to automatically coalesce the RDD so that each partition has approximately the given partition size in bytes.
            partition_size: if auto_coalesce is enable, coalesce the RDD so that each partition has approximately the given partition size in bytes.
            shuffle: if auto_coalesce is enable, whether to shuffle the RDD.
            min_num_partitions: if auto_coalesce is enable and this variable is not None, this will be the minimum number of partitions to coalesce to.
            max_num_partitions: if auto_coalesce is enable and this variable is not None, this will be the maximum number of partitions to coalesce to.
        """
        outdir = str(outdir)

        if not auto_coalesce:
            self.rdd.saveAsTextFile(outdir, compressionCodecClass=compressionCodecClass)
        else:
            tmp_dir = str(outdir) + "_tmp"
            self.rdd.saveAsTextFile(
                tmp_dir, compressionCodecClass=compressionCodecClass
            )

            rdd = get_spark_context().textFile(tmp_dir)
            num_partitions = math.ceil(
                sum((os.path.getsize(file) for file in glob.glob(tmp_dir + "/part-*")))
                / partition_size
            )
            num_partitions = max(min_num_partitions, num_partitions)
            num_partitions = min(max_num_partitions, num_partitions)

            rdd.coalesce(num_partitions, shuffle).saveAsTextFile(
                outdir, compressionCodecClass=compressionCodecClass
            )
            shutil.rmtree(tmp_dir)

        name = name or os.path.basename(outdir)
        if checksum:
            # compute checksum and save it to a file -- reload from the file so we do not have to process the data again.
            ds_checksum = ExtendedRDD(
                get_spark_context().textFile(outdir), self.sig
            ).hash()
        else:
            ds_checksum = b"\x00" * 32

        if self.sig.name != NEW_DATASET_NAME:
            sig = DatasetSignature(
                name=name,
                created_at=str(datetime.now().astimezone()),
                checksum=ds_checksum.hex(),
                dependencies={self.sig.name: self.sig},
            )
        else:
            assert self.sig.created_at == "" and self.sig.checksum == ""
            sig = DatasetSignature(
                name=name,
                created_at=str(datetime.now().astimezone()),
                checksum=ds_checksum.hex(),
                dependencies=self.sig.dependencies.copy(),
            )
        assert sig.is_valid()

        serde.json.ser(sig.to_dict(), os.path.join(outdir, "_SIGNATURE"), indent=2)

    def hash(self: ExtendedRDD[str] | ExtendedRDD[bytes]):
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
            val = int.from_bytes(
                hash1, byteorder="little", signed=False
            ) + int.from_bytes(hash2, byteorder="little", signed=False)
            return (
                val % int.from_bytes(maxint, byteorder="little", signed=False)
            ).to_bytes(32, byteorder="little")

        return self.rdd.map(hash).fold(zero, sum_hash)

    def create_sig(
        self: ExtendedRDD[str] | ExtendedRDD[bytes],
        name: str,
        deps: list[DatasetSignature],
        checksum: bool = True,
    ) -> DatasetSignature:
        ddeps = {}
        for dep in deps:
            assert dep.name not in ddeps and dep.name != NEW_DATASET_NAME
            ddeps[dep.name] = dep
        sig = DatasetSignature(
            name=name,
            created_at=str(datetime.now().astimezone()),
            checksum=self.hash().hex() if checksum else NO_CHECKSUM,
            dependencies=ddeps,
        )
        assert sig.is_valid()
        return sig

    def filter_update_type(
        self: ExtendedRDD[T], f: Callable[[T], TypeGuard[U]]
    ) -> ExtendedRDD[U]:
        return ExtendedRDD(self.rdd.filter(f), self.sig)  # type: ignore

    def left_outer_join_repartition(
        self: ExtendedRDD[tuple[K, V]],
        other: ExtendedRDD[tuple[K, V2]],
        threshold: int = 10000,
        batch_size: int = 1000,
        num_partitions: Optional[int] = None,
    ):
        """This join is useful in the following scenario:

        1. rdd1 contains **duplicated** keys, and potentially high cardinality keys
        2. rdd2 contains **unique** keys

        To avoid high cardinality keys, we artificially generate new keys that have the following format (key, category)
        where category is a number between [1, n], then perform the join.
        """
        return ExtendedRDD(
            left_outer_join_repartition(
                self.rdd, other.rdd, threshold, batch_size, num_partitions
            ),
            self.sig.use(other.sig),
        )

    def join_repartition(
        self: ExtendedRDD[tuple[K, V]],
        other: ExtendedRDD[tuple[K, V2]],
        threshold: int = 10000,
        batch_size: int = 1000,
        num_partitions: Optional[int] = None,
    ):
        """This join is useful in the following scenario:

        1. rdd1 contains **duplicated** keys, and potentially high cardinality keys
        2. rdd2 contains **unique** keys

        To avoid high cardinality keys, we artificially generate new keys that have the following format (key, category)
        where category is a number between [1, n], then perform the join.
        """
        return ExtendedRDD(
            join_repartition(
                self.rdd, other.rdd, threshold, batch_size, num_partitions
            ),
            self.sig.use(other.sig),
        )

    def is_unique(
        self,
        keyfn: Callable[[T_co], Union[str, int]],
        print_error: bool = True,
        return_duplication: bool = False,
    ):
        return are_records_unique(self.rdd, keyfn, print_error, return_duplication)

    # ======================================================================

    @staticmethod
    def textFile(
        indir: StrPath, minPartitions: Optional[int] = None, use_unicode: bool = True
    ):
        sigfile = Path(indir) / "_SIGNATURE"
        if sigfile.exists():
            sig = serde.json.deser(sigfile, DatasetSignature)
            assert sig.is_valid()
        else:
            sig = DatasetSignature(
                name=NEW_DATASET_NAME,
                created_at="",
                checksum="",
                dependencies={},
            )
        return ExtendedRDD(
            get_spark_context().textFile(str(indir), minPartitions, use_unicode), sig
        )

    @staticmethod
    def binaryFiles(
        path: StrPath, minPartitions: Optional[int] = None
    ) -> ExtendedRDD[tuple[str, bytes]]:
        sigfile = Path(path) / "_SIGNATURE"
        if sigfile.exists():
            sig = serde.json.deser(sigfile, DatasetSignature)
            assert sig.is_valid()
        else:
            sig = DatasetSignature(
                name=NEW_DATASET_NAME,
                created_at="",
                checksum="",
                dependencies={},
            )

        return ExtendedRDD(
            get_spark_context().binaryFiles(str(path), minPartitions), sig
        )

    @staticmethod
    def parallelize(
        lst: Sequence[T_co],
        numSlices: Optional[int] = None,
        sig: Optional[DatasetSignature] = None,
    ) -> ExtendedRDD[T_co]:
        sig = sig or DatasetSignature(
            name=NEW_DATASET_NAME,
            created_at="",
            checksum="",
            dependencies={},
        )
        return ExtendedRDD(
            get_spark_context().parallelize(lst, numSlices),
            DatasetSignature.intermediate_dataset("parallelize"),
        )

    def take(self: ExtendedRDD[T], num: int) -> list[T]:
        return self.rdd.take(num)

    def union(self: ExtendedRDD[T], other: ExtendedRDD[U]) -> ExtendedRDD[T | U]:
        return ExtendedRDD(self.rdd.union(other.rdd), self.sig.use(other.sig))

    def map(
        self: ExtendedRDD[T], f: Callable[[T], U], preservesPartitioning: bool = False
    ) -> ExtendedRDD[U]:
        return ExtendedRDD(self.rdd.map(f, preservesPartitioning), self.sig)

    def flatMap(
        self: ExtendedRDD[T],
        f: Callable[[T], Iterable[U]],
        preservesPartitioning: bool = False,
    ) -> ExtendedRDD[U]:
        return ExtendedRDD(self.rdd.flatMap(f, preservesPartitioning), self.sig)

    def filter(self: ExtendedRDD[T], f: Callable[[T], bool]) -> ExtendedRDD[T]:
        return ExtendedRDD(self.rdd.filter(f), self.sig)

    def groupByKey(
        self: ExtendedRDD[tuple[K, V]],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> ExtendedRDD[tuple[K, Iterable[V]]]:
        return ExtendedRDD(self.rdd.groupByKey(numPartitions, partitionFunc), self.sig)

    def distinct(
        self: ExtendedRDD[T], numPartitions: Optional[int] = None
    ) -> ExtendedRDD[T]:
        return ExtendedRDD(self.rdd.distinct(numPartitions), self.sig)

    def groupBy(
        self: ExtendedRDD[T],
        f: Callable[[T], K],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> ExtendedRDD[tuple[K, Iterable[T]]]:
        return ExtendedRDD(self.rdd.groupBy(f, numPartitions, partitionFunc), self.sig)

    def join(
        self: ExtendedRDD[tuple[K, V]],
        other: ExtendedRDD[tuple[K, U]],
        numPartitions: Optional[int] = None,
    ) -> ExtendedRDD[tuple[K, tuple[V, U]]]:
        return ExtendedRDD(
            self.rdd.join(other.rdd, numPartitions), self.sig.use(other.sig)
        )

    def fullOuterJoin(
        self: ExtendedRDD[tuple[K, V]],
        other: ExtendedRDD[tuple[K, U]],
        numPartitions: Optional[int] = None,
    ) -> ExtendedRDD[tuple[K, tuple[Optional[V], Optional[U]]]]:
        return ExtendedRDD(
            self.rdd.fullOuterJoin(other.rdd, numPartitions), self.sig.use(other.sig)
        )

    def leftOuterJoin(
        self: ExtendedRDD[tuple[K, V]],
        other: ExtendedRDD[tuple[K, U]],
        numPartitions: Optional[int] = None,
    ) -> ExtendedRDD[tuple[K, tuple[V, Optional[U]]]]:
        return ExtendedRDD(
            self.rdd.leftOuterJoin(other.rdd, numPartitions), self.sig.use(other.sig)
        )

    def reduceByKey(
        self: ExtendedRDD[tuple[K, V]],
        func: Callable[[V, V], V],
        numPartitions: Optional[int] = None,
        partitionFunc: Callable[[K], int] = portable_hash,
    ) -> ExtendedRDD[tuple[K, V]]:
        return ExtendedRDD(
            self.rdd.reduceByKey(func, numPartitions, partitionFunc), self.sig
        )

    def sortByKey(
        self: ExtendedRDD[tuple[K, V]],
        ascending: Optional[bool] = True,
        numPartitions: Optional[int] = None,
        keyfunc: Callable[[K], S] = lambda x: x,  # type: ignore
    ) -> ExtendedRDD[tuple[K, V]]:
        return ExtendedRDD(
            self.rdd.sortByKey(ascending, numPartitions, keyfunc),  # type: ignore
            self.sig,
        )

    def sortBy(
        self: ExtendedRDD[T],
        keyfunc: Callable[[T], S],
        ascending: bool = True,
        numPartitions: Optional[int] = None,
    ) -> ExtendedRDD[T]:
        return ExtendedRDD(self.rdd.sortBy(keyfunc, ascending, numPartitions), self.sig)

    def subtract(
        self: ExtendedRDD[T], other: ExtendedRDD[T], numPartitions: Optional[int] = None
    ) -> ExtendedRDD[T]:
        return ExtendedRDD(
            self.rdd.subtract(other.rdd, numPartitions), self.sig.use(other.sig)
        )

    def coalesce(
        self: ExtendedRDD[T], numPartitions: int, shuffle: bool = False
    ) -> ExtendedRDD[T]:
        return ExtendedRDD(self.rdd.coalesce(numPartitions, shuffle), self.sig)

    def zipWithIndex(self: ExtendedRDD[T]) -> ExtendedRDD[tuple[T, int]]:
        return ExtendedRDD(self.rdd.zipWithIndex(), self.sig)

    def repartition(self: ExtendedRDD[T], numPartitions: int) -> ExtendedRDD[T]:
        return ExtendedRDD(self.rdd.repartition(numPartitions), self.sig)
