from __future__ import annotations
from dataclasses import dataclass
import glob
from pathlib import Path
from typing import Callable, Generic, List, Tuple, TypeVar, Union, Optional, cast

from kgdata.spark import get_spark_context
from pyspark import RDD
from sm.misc import deserialize_lines
from hugedict.misc import identity, Chain2
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
        return Dataset(
            file_pattern,
            deserialize=identity,
            prefilter=None,
            is_deser_identity=True,
        )

    def get_files(self) -> List[str]:
        return glob.glob(str(self.file_pattern))

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

    def get_dict(self: Dataset[Tuple[str, str]], rstrip: bool = True):
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = {}
        if rstrip:
            for file in tqdm(self.get_files(), desc="read dataset"):
                for line in deserialize_lines(file):
                    k, v = self.deserialize(line.rstrip())
                    output[k] = v
        else:
            for file in tqdm(self.get_files(), desc="read dataset"):
                for line in deserialize_lines(file):
                    k, v = self.deserialize(line)
                    output[k] = v
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
            deserialize=Chain2(func, self.deserialize),
            prefilter=self.prefilter,
            is_deser_identity=False,
        )
