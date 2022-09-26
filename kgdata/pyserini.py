"""Utilities for Pyserini."""

from __future__ import annotations
from dataclasses import asdict, dataclass
from enum import Enum
from functools import partial
import os
from pathlib import Path
from typing import Callable, TypedDict, Union
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.db import get_wdprop_db
from pyserini.search.lucene import LuceneSearcher
import orjson

from pyserini.analysis import Analyzer as PyseriniAnalyzer, get_lucene_analyzer
from kgdata.wikidata.datasets.properties import properties
from pyserini.index.lucene import IndexReader
from pyspark import RDD
from sm.misc.deser import deserialize_json, serialize_json
from sm.misc.funcs import identity_func


class AnalyzerType(str, Enum):
    DefaultEnglishAnalyzer = "default_analyzer"
    TrigramAnalyzer = "trigram_analyzer"


@dataclass(eq=True)
class IndexSettings:
    analyzer: AnalyzerType = AnalyzerType.TrigramAnalyzer

    def get_analyzer(self):
        if self.analyzer == AnalyzerType.DefaultEnglishAnalyzer:
            return identity_func
        if self.analyzer == AnalyzerType.TrigramAnalyzer:
            return TrigramAnalyzer()
        raise NotImplementedError(f"Analyzer type {self.analyzer} is not implemented")

    def to_dict(self) -> dict:
        return {
            "version": 1,
            "analyzer": self.analyzer.value,
        }

    @staticmethod
    def from_dict(o: dict) -> IndexSettings:
        assert o["version"] == 1
        return IndexSettings(analyzer=AnalyzerType(o["analyzer"]))


class TrigramAnalyzer:
    def __init__(self):
        self.pyserini_analyzer = PyseriniAnalyzer(
            get_lucene_analyzer(
                language="en", stemming=True, stemmer="krovetz", stopwords=True
            )
        )

    def __call__(self, text: str):
        return " ".join(
            [
                ngram
                for token in self.pyserini_analyzer.analyze(text)
                for ngram in self.ngram_filter(token, min=3, max=4)
            ]
        )

    @staticmethod
    def ngram_filter(text, min, max):
        if len(text) <= min:
            return [text]

        size = len(text)
        output = []
        for start in range(0, size - min + 1):
            for nchar in range(min, max + 1):
                end = start + nchar
                if end > size:
                    break
                output.append(text[start:end])
        return output

    def __getstate__(self):
        return {}

    def __setstate__(self, state):
        self.__init__()


class PyseriniDoc(TypedDict):
    id: str
    contents: str
    aliases: str
    pagerank: float


def retokenize(doc: PyseriniDoc, analyzer: Callable[[str], str]):
    doc["contents"] = analyzer(doc["contents"])
    doc["aliases"] = analyzer(doc["aliases"])
    return doc


def build_pyserini_index(
    dataset: RDD[PyseriniDoc],
    data_dir: Union[str, Path],
    index_dir: Union[str, Path],
    settings: IndexSettings,
    n_files: int = 256,
    optimize: bool = True,
):
    """Build a pyserini index from the given dataset.

    Args:
        dataset: The dataset to build the index from.
        data_dir: The directory to store the serialized docs.
        index_dir: The directory to store the index.
        settings: The settings to use for the index.
        n_files: The number of files to store the docs (to avoid huge number of files).
        optimize: Whether to optimize the index.
    """
    data_dir = Path(data_dir)
    index_dir = Path(index_dir)

    if not does_result_dir_exist(data_dir):
        rdd = dataset.map(partial(retokenize, analyzer=settings.get_analyzer())).map(
            orjson.dumps
        )

        if n_files > 0:
            rdd = rdd.coalesce(n_files)

        rdd.saveAsTextFile(str(data_dir))

        for file in Path(data_dir).iterdir():
            if file.name.startswith("part-"):
                file.rename(file.parent / f"{file.name}.jsonl")

        serialize_json(settings.to_dict(), data_dir / "_SUCCESS", indent=4)
    else:
        assert (
            IndexSettings.from_dict(deserialize_json(data_dir / "_SUCCESS")) == settings
        )

    if not does_result_dir_exist(index_dir):
        from jnius import autoclass

        index_dir.mkdir(parents=True, exist_ok=True)
        extra_args = []
        if settings.analyzer != AnalyzerType.DefaultEnglishAnalyzer:
            extra_args.append(f"-pretokenized")

        if optimize:
            extra_args.append("-optimize")

        JIndexCollection = autoclass("io.anserini.index.IndexCollection")
        JIndexCollection.main(
            [
                "-collection",
                "JsonCollection",
                "-input",
                str(data_dir),
                "-index",
                str(index_dir),
                "-generator",
                "DefaultLuceneDocumentGenerator",
                "-storePositions",
                "-storeDocvectors",
                "-storeRaw",
                "-threads",
                str(os.cpu_count()),
                "-memorybuffer",
                "8192",
                "-fields",
                "aliases",
            ]
            + extra_args
        )
        (index_dir / "_SUCCESS").touch()
        serialize_json(settings.to_dict(), index_dir / "_SUCCESS", indent=4)
    else:
        assert (
            IndexSettings.from_dict(deserialize_json(index_dir / "_SUCCESS"))
            == settings
        )
