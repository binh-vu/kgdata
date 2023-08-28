from __future__ import annotations

import glob
import os
import re
import shutil
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Literal,
    Optional,
    TypeVar,
    Union,
    cast,
)
from uuid import uuid4

import serde.byteline
import serde.json
import serde.textline
from hugedict.misc import Chain2, identity
from loguru import logger
from pyspark import RDD
from tqdm.auto import tqdm

from kgdata.spark import ExtendedRDD, SparkLikeInterface, get_spark_context
from kgdata.spark.common import does_result_dir_exist
from kgdata.spark.extended_rdd import DatasetSignature

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

    name: Optional[str] = None
    dependencies: Optional[list[Dataset]] = None

    @staticmethod
    def string(
        file_pattern: Union[str, Path],
        name: Optional[str] = None,
        dependencies: Optional[list[Dataset]] = None,
    ) -> Dataset[str]:
        """Create a dataset of string."""
        return Dataset(
            file_pattern,
            deserialize=identity,
            prefilter=None,
            is_deser_identity=True,
            name=name,
            dependencies=dependencies,
        )

    def get_name(self) -> str:
        assert self.name is not None
        return self.name

    def get_dependencies(self) -> list[Dataset]:
        assert self.dependencies is not None
        return self.dependencies

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

    def get_extended_rdd(self) -> ExtendedRDD[T_co]:
        return ExtendedRDD(self.get_rdd(), self.get_signature())

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
    ) -> SparkLikeInterface[T_co]:
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
    ) -> list[T_co]:
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

    def get_set(
        self, rstrip: bool = True, file_order: Optional[Literal["asc", "desc"]] = None
    ) -> set[T_co]:
        assert (
            self.prefilter is None and self.postfilter is None
        ), "Does not support filtering for non-rdd usage yet."
        output = set()
        if rstrip:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.add(self.deserialize(line.rstrip()))
        else:
            for file in tqdm(self.get_files(file_order), desc="read dataset"):
                for line in serde.textline.deser(file):
                    output.add(self.deserialize(line))
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

    def get_signature(self) -> DatasetSignature:
        """Return signature of the dataset. Only works if the file_pattern are in the form of /path/to/files/*.gz (spark format)"""
        metadata = self.get_data_directory() / "_SIGNATURE"
        signature = serde.json.deser(metadata, DatasetSignature)
        assert signature.is_valid()
        return signature

    def sign(
        self,
        name: str,
        deps: Optional[list[DatasetSignature] | list[Dataset]] = None,
        checksum: bool = True,
        mark_success: bool = True,
    ):
        """Create a signature of the dataset (and mark them as success if hasn't been done so)"""
        sig = ExtendedRDD.textFile(self.file_pattern).create_sig(
            name,
            [
                dep if isinstance(dep, DatasetSignature) else dep.get_signature()
                for dep in (deps or [])
            ],
            checksum=checksum,
        )
        outdir = self.get_data_directory()
        serde.json.ser(sig.to_dict(), outdir / "_SIGNATURE", indent=2)

        if mark_success:
            if not (outdir / "_SUCCESS").exists():
                (outdir / "_SUCCESS").touch()

    def get_data_directory(self) -> Path:
        """Return the directory containing the data. It supports two formats:
        - <indir>/*.<extension> or <indir>/part-* (for spark) where extension is in [".gz"]
        - <indir>/*/*.<extension> (for splitting -- then <indir>/_SUCCESS must exist)

        If we use for spark, extension must the .gz
        """
        dirname = os.path.dirname(self.file_pattern)
        pattern = os.path.basename(self.file_pattern)

        if (
            dirname.find("*") == -1
            and re.match(r"^(\*.*)|(part-\*)", pattern) is not None
        ):
            return Path(dirname)

        subdirname = os.path.basename(dirname)
        dirname = os.path.dirname(dirname)

        if (
            dirname.find("*") == -1
            and subdirname == "*"
            and pattern[0] == "*"
            and pattern[1:].find("*") == -1
        ):
            assert (
                Path(dirname) / "_SUCCESS"
            ).exists(), f"{dirname} does not contain _SUCCESS"
            return Path(dirname)

        raise ValueError(f"Cannot infer the data directory from {self.file_pattern}")

    def has_complete_data(
        self,
        need_sig: bool = True,
        allow_override: bool = True,
        create_if_not_exist: bool = False,
    ) -> bool:
        """Check if the indir contains the completed dataset.

        # Arguments
            need_sig: whether to compute the signature of the dataset automatically if missing.
            allow_override: whether to allow override the directory if the result is not success.
            create_if_not_exist: whether to create the directory if it does not exist.
        """
        indir = self.get_data_directory()
        if not does_result_dir_exist(
            indir,
            allow_override=allow_override,
            create_if_not_exist=create_if_not_exist,
        ):
            return False

        if (
            need_sig
            and os.environ.get("KGDATA_FORCE_DISABLE_CHECK_SIGNATURE", "0") == "0"
        ):
            if (indir / "_SIGNATURE").exists():
                signature = serde.json.deser(indir / "_SIGNATURE", DatasetSignature)
                if signature.is_valid():
                    return True

            # TODO: how to construct the signature -- we have no information about the dependents?
            # for now, we can't do it so we just return False -- forcing the user to create the dataset again.
            if allow_override:
                shutil.rmtree(indir)
            if create_if_not_exist:
                Path(indir).mkdir(parents=True)
            return False
        return True


def import_dataset(dataset: str, kwargs: Optional[dict] = None) -> Dataset:
    kgname, dataset = dataset.split(".")
    module = import_module(f"kgdata.{kgname}.datasets.{dataset}")
    kwargs = kwargs or {}
    return getattr(module, dataset)(**kwargs)
