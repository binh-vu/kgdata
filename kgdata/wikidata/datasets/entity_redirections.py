from __future__ import annotations

import shutil
from collections import defaultdict
from functools import lru_cache
from typing import Dict

import serde.csv
import serde.textline
from sm.misc.funcs import is_not_null
from tqdm import tqdm

from kgdata.dataset import Dataset
from kgdata.misc.funcs import split_tab_2
from kgdata.spark.extended_rdd import ExtendedRDD
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entity_ids import entity_ids, is_entity_id
from kgdata.wikidata.datasets.entity_redirection_dump import entity_redirection_dump
from kgdata.wikidata.datasets.page_ids import page_ids, parse_sql_values


@lru_cache()
def entity_redirections() -> Dataset[tuple[str, str]]:
    """Wikidata entity redirections. It combines two datasets: page_ids and entity_redirection_dump.
    The first one contains mapping from page_id => entity_id (can be old id).
    The second one contains mapping from page_id => final entity_id.

    We did a join between two datasets based on page_id so we can get the mapping from old entity_id to the final entity_id.

    Finally, we check if the final entity id is in the entity_ids dataset. If not, we remove the mapping.

    Returns:
        Dataset[tuple[str, str]]
    """
    cfg = WikidataDirCfg.get_instance()

    raw_ds = Dataset(
        cfg.entity_redirections / "raw/part-*",
        deserialize=split_tab_2,
        name="entity-redirections/raw",
        dependencies=[entity_redirection_dump()],
    )
    redirection_ds = Dataset(
        cfg.entity_redirections / "redirections/*.tsv",
        deserialize=split_tab_2,
        name="entity-redirections/redirections",
        dependencies=[page_ids()],
    )
    fixed_ds = Dataset(
        cfg.entity_redirections / "fixed/*.tsv",
        deserialize=split_tab_2,
        name="entity-redirections/fixed",
        # to not store intermediate dependencies
        dependencies=raw_ds.get_dependencies()
        + redirection_ds.get_dependencies()
        + [entity_ids()],
    )

    if not raw_ds.has_complete_data():
        # mapping from page id to the latest entity id
        (
            entity_redirection_dump()
            .get_extended_rdd()
            .flatMap(parse_sql_values)
            .map(extract_id)
            .filter_update_type(is_not_null)
            .map(lambda x: "\t".join(x))
            .coalesce(1)
            .save_like_dataset(raw_ds)
        )

    if not redirection_ds.has_complete_data():
        page2ent = page_ids().get_dict()
        (raw_redirect_file,) = raw_ds.get_files()
        raw_redirections = serde.csv.deser(raw_redirect_file, delimiter="\t")

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

        redirection_ds.get_data_directory().mkdir(parents=True, exist_ok=True)
        serde.csv.ser(
            [
                [before, after]
                for before, after in sorted(refined_redirections.items())
                if before != after  # there is a case where this is equal: P2094
            ],
            redirection_ds.get_data_directory() / "redirection.tsv",
            delimiter="\t",
        )
        redirection_ds.sign(
            redirection_ds.get_name(), redirection_ds.get_dependencies()
        )

    if not fixed_ds.has_complete_data():
        (redirection_file,) = redirection_ds.get_files()
        lst = serde.csv.deser(redirection_file, delimiter="\t")

        unk_target_ds = Dataset.string(
            cfg.entity_redirections / "unknown_target_entities/part-*",
            name="entity-redirections/unknown-target-entities",
            dependencies=[redirection_ds, entity_ids()],
        )

        if not unk_target_ds.has_complete_data():
            (
                ExtendedRDD.parallelize(list(set(x[1] for x in lst)))
                .subtract(entity_ids().get_extended_rdd())
                .save_like_dataset(
                    dataset=unk_target_ds,
                    checksum=False,
                    auto_coalesce=True,
                    trust_dataset_dependencies=True,
                )
            )

        unknown_ents = unk_target_ds.get_set()
        fixed_ds.get_data_directory().mkdir(parents=True, exist_ok=True)
        if len(unknown_ents) > 0:
            serde.csv.ser(
                [[before, after] for before, after in lst if after not in unknown_ents],
                fixed_ds.get_data_directory() / "fixed_redirections.tsv",
                delimiter="\t",
            )
        else:
            shutil.copyfile(
                redirection_file,
                fixed_ds.get_data_directory() / "fixed_redirections.tsv",
            )
        fixed_ds.sign(fixed_ds.get_name(), fixed_ds.get_dependencies())

    return fixed_ds


def extract_id(row: list) -> tuple[str, str] | None:
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
