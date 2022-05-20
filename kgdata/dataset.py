from __future__ import annotations
from dataclasses import dataclass
import glob
from pathlib import Path
from typing import Callable, Generic, List, Tuple, TypeVar, Union, Optional

from kgdata.spark import get_spark_context
from sm.misc import deserialize_byte_lines, deserialize_lines, identity_func
from tqdm import tqdm

V = TypeVar("V")


@dataclass
class Dataset(Generic[V]):
    # pattern to files (e.g., /*.gz)
    file_pattern: Union[str, Path]
    deserialize: Callable[[str], V]

    # whether the deserialize function is an identity function
    # only happens when is this a list of string
    is_deser_identity: bool = False

    @staticmethod
    def string(file_pattern: Union[str, Path]) -> Dataset[str]:
        return Dataset(file_pattern, deserialize=identity_func, is_deser_identity=True)

    def get_files(self) -> List[str]:
        return glob.glob(str(self.file_pattern))

    def get_rdd(self):
        rdd = get_spark_context().textFile(str(self.file_pattern))
        if not self.is_deser_identity:
            return rdd.map(self.deserialize)
        return rdd

    def get_dict(self: Dataset[Tuple[str, str]], rstrip: bool = True):
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
