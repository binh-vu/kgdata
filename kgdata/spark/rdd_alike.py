from __future__ import annotations

from math import ceil
from pathlib import Path
from typing import (
    Callable,
    Generic,
    Hashable,
    Iterable,
    Optional,
    TypeVar,
    cast,
)

from kgdata.splitter import split_a_list
from tqdm.auto import tqdm


V = TypeVar("V")
V2 = TypeVar("V2")
T_co = TypeVar("T_co", covariant=True)
K = TypeVar("K", bound=Hashable)

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