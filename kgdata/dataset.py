from __future__ import annotations

import glob
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    Literal,
    Optional,
    TypeVar,
    Union,
    cast,
)

import orjson
import serde.byteline
import serde.textline
from loguru import logger
from pyspark import RDD
from tqdm.auto import tqdm

from hugedict.misc import Chain2, identity
from kgdata.spark import get_spark_context
from kgdata.splitter import split_a_list

V = TypeVar("V")
V2 = TypeVar("V2")
T_co = TypeVar("T_co", covariant=True)
K = TypeVar("K", bound=Hashable)


@dataclass
class Dataset(Generic[T_co]):
    # pattern to files (e.g., /*.gz)
    file_pattern: Union[str, Path]
    deserialize: Callable[[str], T_co]
    # this filter function is applied before deserialization
    prefilter: Optional[Callable[[str], bool]] = None
    # this filter function is applied after deserialization
    postfilter: Optional[Callable[[T_co], bool]] = None

    # whether the deserialize function is an identity function
    # only happens when is this a list of string
    # just to avoid unnecessary function calls
    is_deser_identity: bool = False

    @staticmethod
    def string(file_pattern: Union[str, Path]) -> Dataset[str]:
        """Create a dataset of string."""
        return Dataset(
            file_pattern,
            deserialize=identity,
            prefilter=None,
            is_deser_identity=True,
        )

    def get_files(
        self, file_order: Optional[Literal["asc", "desc"]] = None
    ) -> list[str]:
        files = glob.glob(str(self.file_pattern))
        if file_order is not None:
            files.sort(reverse=file_order == "desc")

        if len(files) == 0:
            logger.warning(f"No files found for {self.file_pattern}")
        return files

    def get_rdd(self) -> RDD[T_co]:
        rdd = get_spark_context().textFile(str(self.file_pattern))
        if self.prefilter is not None:
            rdd = rdd.filter(self.prefilter)

        if not self.is_deser_identity:
            rdd = rdd.map(self.deserialize)
        else:
            rdd = cast(RDD[T_co], rdd)

        if self.postfilter is not None:
            rdd = rdd.filter(self.postfilter)

        return rdd

    def take(
        self,
        n: int,
        rstrip: bool = True,
        file_order: Optional[Literal["asc", "desc"]] = None,
    ):
        output = []
        for file in tqdm(self.get_files(file_order), desc="read dataset"):
            for line in serde.textline.deser(file):
                output.append(self.deserialize(line.rstrip()))
                if len(output) >= n:
                    break
            if len(output) >= n:
                break
        return output

    def get_rdd_alike(
        self, rstrip: bool = True, file_order: Optional[Literal["asc", "desc"]] = None
    ) -> SparkLikeInterface[V]:
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = []
        if rstrip:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.append(self.deserialize(line.rstrip()))
        else:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.append(self.deserialize(line))
        return SparkLikeInterface(output)

    def get_list(
        self, rstrip: bool = True, file_order: Optional[Literal["asc", "desc"]] = None
    ):
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = []
        if rstrip:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.append(self.deserialize(line.rstrip()))
        else:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.append(self.deserialize(line))
        return output

    def get_dict(
        self: Dataset[tuple[K, V]],
        rstrip: bool = True,
        file_order: Optional[Literal["asc", "desc"]] = None,
    ) -> dict[K, V]:
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = {}
        if rstrip:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    k, v = self.deserialize(line.rstrip())
                    output[k] = v
        else:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    k, v = self.deserialize(line)
                    output[k] = v
        return output

    def get_dict_items(
        self: Dataset[tuple[str, str]],
        rstrip: bool = True,
        file_order: Optional[Literal["asc", "desc"]] = None,
    ):
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = []
        if rstrip:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    k, v = self.deserialize(line.rstrip())
                    output.append((k, v))
        else:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    k, v = self.deserialize(line)
                    output.append((k, v))
        return output

    def does_exist(self) -> bool:
        return len(self.get_files()) > 0

    def map(self, func: Callable[[T_co], V2]) -> Dataset[V2]:
        """Transform record from its origin type to another type.

        Args:
            func: transformation function
        """
        return Dataset(
            file_pattern=self.file_pattern,
            deserialize=Chain2(func, self.deserialize)
            if self.deserialize is not identity
            else func,  # type: ignore
            prefilter=self.prefilter,
            postfilter=self.postfilter,
            is_deser_identity=False,
        )

    def filter(self, func: Callable[[T_co], bool]) -> Dataset[T_co]:
        """Filter record by a function.

        Args:
            func: filter function
        """
        return Dataset(
            file_pattern=self.file_pattern,
            deserialize=self.deserialize,
            prefilter=self.prefilter,
            postfilter=Chain2(func, self.postfilter),
            is_deser_identity=self.is_deser_identity,
        )

    @staticmethod
    def save_to_files(
        records: Union[list[str], list[bytes]],
        outdir: Path,
        n_records_per_file: int = 10000,
        verbose: bool = False,
    ):
        if len(records) == 0:
            return

        if not outdir.exists():
            outdir.mkdir(parents=True)

        serialize_fn = (
            serde.byteline.ser if isinstance(records[0], bytes) else serde.textline.ser
        )

        for no, i in tqdm(
            enumerate(range(0, len(records), n_records_per_file)), disable=not verbose
        ):
            batch = records[i : i + n_records_per_file]
            serialize_fn(cast(Any, batch), outdir / f"part-{no:05d}.gz")


class SparkLikeInterface(Generic[T_co]):
    """An implementation of Spark-like interface for non-distributed usage."""

    def __init__(self, data: list | dict, n_partitions: int = 1):
        self.data = data
        self.size = len(data)
        self.n_partitions = n_partitions

    def _iter_records(self) -> Iterable[T_co]:
        return cast(
            Iterable[T_co],
            tqdm(
                self.data if isinstance(self.data, list) else self.data.items(),
                total=self.size,
                leave=False,
            ),
        )

    def map(self, func: Callable[[T_co], V2]) -> SparkLikeInterface[V2]:
        return SparkLikeInterface(
            [func(x) for x in self._iter_records()], n_partitions=self.n_partitions
        )

    def filter(self, func: Callable[[T_co], bool]) -> SparkLikeInterface[T_co]:
        return SparkLikeInterface(
            [x for x in self._iter_records() if func(x)], n_partitions=self.n_partitions
        )

    def flatMap(self, func: Callable[[T_co], Iterable[V2]]) -> SparkLikeInterface[V2]:
        return SparkLikeInterface(
            [y for x in self._iter_records() for y in func(x)],
            n_partitions=self.n_partitions,
        )

    def union(self, data: list) -> SparkLikeInterface[T_co]:
        assert isinstance(self.data, list)
        return SparkLikeInterface(self.data + data, n_partitions=self.n_partitions)

    def groupByKey(
        self: SparkLikeInterface[tuple[K, V]],
    ) -> SparkLikeInterface[tuple[K, list[V]]]:
        obj = {}
        for k, v in self._iter_records():
            if k not in obj:
                obj[k] = []
            obj[k].append(v)
        return SparkLikeInterface(obj, n_partitions=self.n_partitions)

    def leftOuterJoin(
        self: SparkLikeInterface[tuple[K, V]],
        other: SparkLikeInterface[tuple[K, V2]],
    ) -> SparkLikeInterface[tuple[K, tuple[V, Optional[V2]]]]:
        obj = {}
        for k, v in self._iter_records():
            obj[k] = (v, None)
        for k, v in other._iter_records():
            if k not in obj:
                continue
            else:
                obj[k] = (obj[k][0], v)
        return SparkLikeInterface(obj, n_partitions=self.n_partitions)

    def join(
        self: SparkLikeInterface[tuple[K, V]],
        other: SparkLikeInterface[tuple[K, V2]],
    ) -> SparkLikeInterface[tuple[K, tuple[V, V2]]]:
        obj = {}
        for k, v in self._iter_records():
            obj[k] = (v, None)
        for k, v in other._iter_records():
            if k not in obj:
                continue
            else:
                obj[k] = (obj[k][0], v)
        for k in list(obj.keys()):
            if obj[k][1] is None:
                del obj[k]
        return SparkLikeInterface(obj, n_partitions=self.n_partitions)

    def coalesce(self, n: int) -> SparkLikeInterface[T_co]:
        return SparkLikeInterface(self.data, n_partitions=n)

    def collect(self) -> list[T_co]:
        if isinstance(self.data, list):
            return self.data
        else:
            return list(self._iter_records())

    def saveAsTextFile(
        self: SparkLikeInterface[str] | SparkLikeInterface[bytes],
        outdir: str | Path,
        compressionCodecClass: str,
    ):
        assert isinstance(self.data, list)
        is_bytes = isinstance(next(iter(self._iter_records())), bytes)

        if not is_bytes:
            data = [x.encode() for x in self.data]
        else:
            data = self.data

        if compressionCodecClass == "org.apache.hadoop.io.compress.GzipCodec":
            outfile = Path(outdir) / "part.gz"
        else:
            outfile = Path(outdir) / "part"

        split_a_list(
            data,
            outfile,
            n_records_per_file=ceil(len(data) / self.n_partitions),
        )
        (Path(outdir) / "_SUCCESS").touch()
