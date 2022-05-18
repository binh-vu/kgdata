from collections import defaultdict
from operator import itemgetter
from typing import Dict, List, Tuple, Union, cast

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import (
    does_result_dir_exist,
    get_spark_context,
    head,
    saveAsSingleTextFile,
)
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dump
from kgdata.wikidata.datasets.entity_ids import is_entity_id
from kgdata.wikidata.datasets.entity_redirection_dump import entity_redirection_dump
from kgdata.dataset import Dataset
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.datasets.page_ids import page_ids, parse_sql_values
import orjson
from pyspark.rdd import RDD
from sm.misc import identity_func, deserialize_jl, deserialize_csv, serialize_csv
from tqdm import tqdm


def entity_redirections() -> Dataset[Tuple[str, str]]:
    """Wikidata entity redirections

    Returns:
        Dataset[tuple[str, str]]
    """
    cfg = WDDataDirCfg.get_instance()

    if not (cfg.entity_redirections / "raw_redirections.tsv").exists():
        # mapping from page id to the latest entity id
        saveAsSingleTextFile(
            entity_redirection_dump()
            .get_rdd()
            .flatMap(parse_sql_values)
            .map(extract_id)
            .filter(lambda x: x is not None)
            .map(lambda x: "\t".join(x)),
            cfg.entity_redirections / "raw_redirections.tsv",
        )

    if not (cfg.entity_redirections / "redirections.tsv").exists():
        page2ent = page_ids().get_dict()
        raw_redirections = deserialize_csv(
            cfg.entity_redirections / "raw_redirections.tsv", delimiter="\t"
        )

        redirections = defaultdict(set)
        for pageid, latest_ent in raw_redirections:
            if pageid not in page2ent:
                # for some page, it is not in the list of page ids
                # don't know why, may be a bug in the dump
                # e.g., P4324
                continue
            old_ent = page2ent[pageid]
            redirections[old_ent].add(latest_ent)

        # now resolve the redirections
        refined_redirections: Dict[str, str] = {}
        for before_item, next_items in tqdm(
            redirections.items(), desc="resolving multi-level redirections"
        ):
            if len(next_items) == 1:
                refined_redirections[before_item] = list(next_items)[0]
                continue

            # there must be only one final item on this list, otherwise, it's not consistent
            final_items = [item for item in next_items if item not in redirections]
            assert len(final_items) == 1, next_items
            final_item = final_items[0]

            # now verify for each item, it is being redirected to the same final item
            stack = [item for item in next_items if item != final_item]
            visited = set()
            while len(stack) > 0:
                item = stack.pop()
                visited.add(item)

                if len(redirections[item]) == 1:
                    if list(redirections[item])[0] != final_item:
                        raise Exception(
                            f"Item {before_item} is mapped to both {final_item} and {item} -> {list(redirections[item])[0]}"
                        )
                    continue

                for next_item in redirections[item]:
                    if next_item in visited:
                        continue
                    if next_item == final_item:
                        continue
                    stack.append(next_item)
            refined_redirections[before_item] = final_item

        serialize_csv(
            cast(List[List[str]], sorted(refined_redirections.items())),
            cfg.entity_redirections / "redirections.tsv",
            delimiter="\t",
        )

    return Dataset(
        file_pattern=cfg.entity_redirections / "redirections.tsv",
        deserialize=lambda x: tuple(x.split("\t")),
    )


def extract_id(row: list):
    # the dumps contain other pages such as user pages, etc.
    page_id, entity_id = row[0], row[2]
    if is_entity_id(entity_id):
        return page_id, entity_id

    return None


def postprocess_groupby(record):
    k, v = record
    v = list(v)
    assert len(v) == len(set(v))
    return k, v
