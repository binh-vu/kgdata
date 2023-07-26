from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generic, Literal, Optional, TypeVar, Union, cast

import orjson
import serde.byteline
import serde.textline
from hugedict.misc import Chain2, identity
from kgdata.spark import get_spark_context
from loguru import logger
from pyspark import RDD
from tqdm import tqdm

V = TypeVar("V")
V2 = TypeVar("V2")


@dataclass
class Dataset(Generic[V]):
    # pattern to files (e.g., /*.gz)
    file_pattern: Union[str, Path]
    deserialize: Callable[[str], V]
    # this filter function is applied before deserialization
    prefilter: Optional[Callable[[str], bool]] = None
    # this filter function is applied after deserialization
    postfilter: Optional[Callable[[V], bool]] = None

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

    def get_rdd(self) -> RDD[V]:
        rdd = get_spark_context().textFile(str(self.file_pattern))
        if self.prefilter is not None:
            rdd = rdd.filter(self.prefilter)

        if not self.is_deser_identity:
            rdd = rdd.map(self.deserialize)
        else:
            rdd = cast(RDD[V], rdd)

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
        self: Dataset[tuple[str, str]],
        rstrip: bool = True,
        file_order: Optional[Literal["asc", "desc"]] = None,
    ):
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

    def map(self, func: Callable[[V], V2]) -> Dataset[V2]:
        """Transform record from its origin type to another type.

        Args:
            func: transformation function
        """
        return Dataset(
            file_pattern=self.file_pattern,
            deserialize=Chain2(func, self.deserialize)
            if self.deserialize is not identity
            else func,
            prefilter=self.prefilter,
            postfilter=self.postfilter,
            is_deser_identity=False,
        )

    def filter(self, func: Callable[[V], bool]) -> Dataset[V]:
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
