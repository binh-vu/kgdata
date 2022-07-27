"""Searching Wikidata using Pyserini Index."""

from dataclasses import dataclass
import os
from pathlib import Path
from typing import List, Literal, Optional, Union
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.classes import classes
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.db import get_wdprop_db
from kgdata.wikidata.models import WDClass, WDEntity, WDProperty
from pyserini.search.lucene import LuceneSearcher
import orjson
from tqdm import tqdm

from kgdata.wikidata.datasets.properties import properties
from pyserini.index.lucene import IndexReader
from sm.misc.deser import deserialize_json
from sm.misc.funcs import identity_func
from kgdata.pyserini import (
    AnalyzerType,
    PyseriniDoc,
    build_pyserini_index,
    IndexSettings,
)


@dataclass
class SearchReturnType:
    """A placeholder for the return type of the search function."""

    docid: str
    score: float


class WDSearch:
    def __init__(self, index: Union[str, Path]):
        self.index_dir = Path(index)
        self.searcher = LuceneSearcher(str(index))
        self.settings = IndexSettings.from_dict(
            deserialize_json(self.index_dir / "_SUCCESS")
        )

        self.analyzer = self.settings.get_analyzer()

    def search(self, query: str, limit: int = 10) -> List[SearchReturnType]:
        res = self.searcher.search(self.analyzer(query), k=limit)
        return [SearchReturnType(docid=x.docid, score=x.score) for x in res]

    def batch_search(
        self, queries: List[str], limit: int = 10
    ) -> List[List[SearchReturnType]]:
        queries = [self.analyzer(query) for query in queries]
        query_ids = [str(x) for x in range(len(queries))]

        n_processes = os.cpu_count()
        assert isinstance(n_processes, int)

        result = self.searcher.batch_search(
            queries, query_ids, k=limit, threads=n_processes
        )
        return [
            [SearchReturnType(docid=x.docid, score=x.score) for x in result[qid]]
            for qid in query_ids
        ]

    def analyze(self, text: str):
        return self.analyzer(text)

    def __getstate__(self):
        return {"index_dir": self.index_dir}

    def __setstate__(self, state):
        self.__init__(state["index_dir"])


def build_index(
    name: Literal["entities", "props", "classes"],
    index_parent_dir: Optional[Union[str, Path]] = None,
    lang: str = "en",
):
    cfg = WDDataDirCfg.get_instance()
    settings = IndexSettings(analyzer=AnalyzerType.TrigramAnalyzer)

    data_dir = cfg.search / name / settings.analyzer.value
    if index_parent_dir is None:
        index_parent_dir = cfg.search
    else:
        index_parent_dir = Path(index_parent_dir)
    index_dir = index_parent_dir / f"{name}_{settings.analyzer}"

    if name == "entities":
        dataset = entities(lang=lang)
    elif name == "props":
        dataset = properties(lang=lang)
    else:
        assert name == "classes"
        dataset = classes(lang=lang)

    dataset = dataset.map(to_doc)
    dataset.postfilter = ignore_empty

    build_pyserini_index(
        dataset,
        data_dir=data_dir,
        index_dir=index_dir,
        settings=settings,
        n_files=-1 if name == "entities" else 256,
    )
    return index_dir


def to_doc(record: Union[WDEntity, WDProperty, WDClass]) -> PyseriniDoc:
    return {"id": record.id, "contents": str(record.label)}


def ignore_empty(record: PyseriniDoc) -> bool:
    return record["contents"].strip() != ""


if __name__ == "__main__":
    cfg = WDDataDirCfg.init("/nas/ckgfs/users/binhvu/wikidata/20211213")

    index = build_index("entities")
    index = build_index("classes")

    index = build_index("props")

    # db = get_wdprop_db(cfg.datadir / "databases/wdprops.db")
    # search = WDSearch(index)
    # hits = search.search("histric coutry", limit=50)
    # for i, hit in enumerate(hits):
    #     print(f"{i+1:2} {hit.score:.5f} {hit.docid:6} {db[hit.docid].label}")

    # from pyserini.index.lucene import IndexReader

    # # Initialize from a pre-built index:
    # index_reader = IndexReader.from_prebuilt_index("robust04")

    # # Initialize from an index path:
    # index_reader = IndexReader("indexes/index-robust04-20191213/")
    # print(ngram_filter("administrative", min=3, max=4))
    # build_wdprops()

    # db = get_wdprop_db("/data/binhvu/sm-dev/data/home/databases/wdprops.db")

    # cfg = WDDataDirCfg.get_instance()

    # index_dir = cfg.search / "properties" / f"index_{IndexSettings.analyzer}"
    # searcher = LuceneSearcher(str(index_dir))

    # def search(q):
    #     hits = searcher.search(q, k=20)

    # search(trigram_analyzer("ocated"))
    # term = "cities"
    # index_reader = IndexReader(str(cfg.search / "properties" / "index"))
    # # Analyze the term.
    # analyzed = index_reader.analyze(term)
    # print(f'The analyzed form of "{term}" is "{analyzed[0]}"')

    # # Skip term analysis:
    # df, cf = index_reader.get_term_counts(analyzed[0], analyzer=None)
    # print(f'term "{term}": df={df}, cf={cf}')
