from __future__ import annotations

import glob
import math
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

import click
import serde.byteline
import serde.json
import serde.textline
from hugedict.misc import Chain2, identity
from loguru import logger
from pyspark import RDD
from tqdm.auto import tqdm

from kgdata.config import init_dbdir_from_env
from kgdata.misc.query import PropQuery, every
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
        verify_dependencies_signature: bool = True,
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
                    # signature is valid -- should we go ahead and verify if the dependencies
                    # is also match?
                    if verify_dependencies_signature:
                        for dep in self.get_dependencies():
                            if (
                                dep.get_signature()
                                != signature.dependencies[dep.get_name()]
                            ):
                                logger.info(
                                    "Signature of a depenency {} does not match what is stored in {}",
                                    dep.get_name(),
                                    self.get_name(),
                                )
                                logger.info(
                                    "Dependency's signature: {}", dep.get_signature()
                                )
                                logger.info(
                                    "Dependency's signature stored in the dataset: {}",
                                    signature.dependencies[dep.get_name()],
                                )
                                raise Exception(
                                    f"The signature of {dep.get_name()} does not match with what is stored in {self.get_name()}. Abort!"
                                )
                    return True

            # TODO: how to construct the signature -- we have no information about the dependents?
            # for now, we can't do it so we just return False -- forcing the user to create the dataset again.
            if allow_override:
                shutil.rmtree(indir)
            if create_if_not_exist:
                Path(indir).mkdir(parents=True)
            return False
        return True

    def get_temporary(self) -> Dataset:
        data_dir = self.get_data_directory()
        data_dir = data_dir.parent / (data_dir.name + "-tmp")

        filepattern = Path(self.file_pattern)
        if filepattern.name == "*.gz":
            new_filepattern = data_dir / "*.gz"
        elif filepattern.name == "part-*":
            new_filepattern = data_dir / "part-*"
        else:
            raise NotImplementedError()

        return Dataset(
            file_pattern=new_filepattern,
            deserialize=self.deserialize,
            prefilter=self.prefilter,
            postfilter=self.postfilter,
            is_deser_identity=self.is_deser_identity,
            name=self.name,
            dependencies=self.dependencies,
        )


def import_dataset(dataset: str, kwargs: Optional[dict] = None) -> Dataset:
    """Import a dataset by name such as wikidata.entities, wikidata.classes, or wikidata.classes.classes.
    If there is only one dot (.) in the dataset, then the function to construct the dataset is expected
    to be the same as the last module name so wikidata.classes.classes is the same as wikidata.classes.
    """
    parts = dataset.split(".")
    if len(parts) == 2:
        kgname, dataset = parts
        dataset_module = dataset
    else:
        assert len(parts) == 3
        kgname, dataset_module, dataset = parts
    module = import_module(f"kgdata.{kgname}.datasets.{dataset_module}")
    kwargs = kwargs or {}
    return getattr(module, dataset)(**kwargs)


@click.command()
@click.argument("dir1")
@click.argument("dir2")
def compare(dir1: Path, dir2: Path):
    dir1 = Path(dir1)
    dir2 = Path(dir2)

    rootdir1 = dir1
    rootdir2 = dir2
    dir1new = []
    dir2new = []
    dirdiff = []
    dirsimi = []

    def _compare(dir1: Path, dir2: Path):
        subdirs1 = {subdir.name: subdir for subdir in dir1.iterdir() if subdir.is_dir()}
        subdirs2 = {subdir.name: subdir for subdir in dir2.iterdir() if subdir.is_dir()}

        # get folders that are new
        for name in set(subdirs1.keys()).difference(subdirs2.keys()):
            dir1new.append(subdirs1[name])

        for name in set(subdirs2.keys()).difference(subdirs1.keys()):
            dir2new.append(subdirs2[name])

        # get folders that are different
        for name in set(subdirs1.keys()).intersection(subdirs2.keys()):
            subdir1 = subdirs1[name]
            subdir2 = subdirs2[name]

            sig1 = None
            if (subdir1 / "_SIGNATURE").exists():
                sig1 = serde.json.deser(subdir1 / "_SIGNATURE")

            sig2 = None
            if (subdir2 / "_SIGNATURE").exists():
                sig2 = serde.json.deser(subdir2 / "_SIGNATURE")

            if sig1 != sig2:
                dirdiff.append(subdir1.relative_to(rootdir1))
                break
            elif sig1 is None:
                # this is not a dataset, we want to compare the content of
                # that directory
                files1 = list(subdir1.iterdir())
                files2 = list(subdir2.iterdir())

                if {f.relative_to(rootdir1) for f in files1} != {
                    f.relative_to(rootdir2) for f in files2
                }:
                    dirdiff.append(subdir1.relative_to(rootdir1))
                    break
                else:
                    for file1 in files1:
                        file2 = subdir2 / file1.name

                        if file1.is_dir():
                            if not file2.is_dir():
                                dirdiff.append(subdir1.relative_to(rootdir1))
                                break
                            else:
                                _compare(file1, file2)
                        elif file2.is_dir():
                            dirdiff.append(subdir1.relative_to(rootdir1))
                            break
            else:
                dirsimi.append((subdir1, subdir2))

    _compare(dir1, dir2)

    logger.info("# Directories that are similar: {}", len(dirsimi))

    if len(dir1new) > 0:
        print("\n")
        logger.info("New directories at: {}", dir1)
        for dir in dir1new:
            print(f"- {dir}")

    if len(dir2new) > 0:
        print("\n")
        logger.info("New directories at: {}", dir2)
        for dir in dir2new:
            print(f"- {dir}")

    if len(dirdiff) > 0:
        print("\n")
        logger.info("Different directories")
        for dir in dirdiff:
            print(f"- {dir}")


def make_dataset_cli(kgname: str):
    @click.command("Generate a specific dataset")
    @click.option("-d", "--dataset", required=True, help="Dataset name")
    @click.option(
        "-s",
        "--sign",
        is_flag=True,
        required=False,
        default=False,
        help="Sign the dataset if it hasn't been signed",
    )
    @click.option(
        "-p",
        "--partition",
        type=int,
        required=False,
        default=0,
        help="Rebalance the dataset so that each partition roughly has the desired MB",
    )
    @click.option(
        "-t", "--take", type=int, required=False, default=0, help="Take n rows"
    )
    @click.option("-q", "--query", type=str, required=False, default="", help="Query")
    @click.option("-l", "--limit", type=int, required=False, default=20, help="Limit")
    def cli(
        dataset: str,
        sign: bool = False,
        partition: int = 0,
        take: int = 0,
        query: str = "",
        limit: int = 20,
    ):
        init_dbdir_from_env()

        if sign:
            # disable signature check
            os.environ["KGDATA_FORCE_DISABLE_CHECK_SIGNATURE"] = "1"

        ds: Dataset = import_dataset(kgname + "." + dataset)

        if sign:
            for dep in ds.get_dependencies():
                # make sure that the dependencies are all signed
                try:
                    dep.get_signature()
                except:
                    print(f"{dep.get_name()} doesn't have a signature")
                    raise

            ds.sign(
                ds.get_name(),
                ds.get_dependencies(),
            )

        if partition > 0:
            files = ds.get_files()
            # get number of partitions, which each has the desired size in MB
            n_partitions = math.ceil(
                sum((os.path.getsize(file) for file in files))
                / (partition * 1024 * 1024)
            )
            if abs(n_partitions - len(files)) > 2:
                print(
                    f"Repartitioning the dataset, change the number of partitions from {len(files)} to {n_partitions}"
                )
                origin_deser = ds.deserialize
                ds.deserialize = identity
                temp_ds = ds.get_temporary()

                origin_data_dir = ds.get_data_directory()
                temp_data_dir = temp_ds.get_data_directory()

                if not temp_ds.has_complete_data():
                    ds.get_extended_rdd().coalesce(
                        n_partitions, shuffle=True
                    ).save_like_dataset(
                        temp_ds,
                        trust_dataset_dependencies=True,
                    )
                ds.deserialize = origin_deser
                # make sure the signature are the same, then remove copy the signature
                ds_sig = ds.get_signature()
                tmp_ds_sig = temp_ds.get_signature()
                tmp_ds_sig = tmp_ds_sig.update(created_at=ds_sig.created_at)
                # assert ds_sig == tmp_ds_sig, (ds_sig.checksum, tmp_ds_sig.checksum)
                assert ds_sig == tmp_ds_sig, (ds_sig, tmp_ds_sig)
                shutil.copy2(
                    origin_data_dir / "_SIGNATURE",
                    temp_data_dir / "_SIGNATURE",
                )
                # then we can remove
                shutil.move(origin_data_dir, str(origin_data_dir) + "-old")
                shutil.move(temp_data_dir, origin_data_dir)
            else:
                print(
                    f"Skip repartitioning dataset, as it has {len(files)} partitions and each partition has roughly {partition} MB"
                )

        if take > 0:
            for record in ds.take(take):
                print(repr(record))
                print("=" * 30)

        if query != "":
            queries = [PropQuery.from_string(s) for s in query.split(",")]
            filter_fn = every(queries)

            for record in ds.get_rdd().filter(filter_fn).take(limit):
                print(repr(record))
                print("=" * 30)

    return cli


if __name__ == "__main__":
    compare()
