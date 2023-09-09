from functools import lru_cache
from operator import itemgetter

from sm.misc.funcs import identity_func
from tqdm import tqdm

from kgdata.dataset import Dataset
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dump


def is_entity_id(id: str) -> bool:
    """Check if id is a Wikidata entity id.

    The implementation of the function is verified using the `entity_ids` dataset.
    """
    return (
        len(id) > 0
        and (id[0] == "Q" or id[0] == "P" or id[0] == "L")
        and id[1:].isdigit()
    )


@lru_cache()
def entity_ids() -> Dataset[str]:
    """Get Wikidata entity ids"""
    cfg = WikidataDirCfg.get_instance()
    id_ds = Dataset.string(
        cfg.entity_ids / "ids/*.gz", name="entity-ids", dependencies=[entity_dump()]
    )

    if not id_ds.has_complete_data():
        (
            entity_dump()
            .get_extended_rdd()
            .map(itemgetter("id"))
            .save_like_dataset(id_ds, auto_coalesce=True)
        )

    if not (cfg.entity_ids / "identifiers.txt").exists():
        (
            id_ds.get_extended_rdd()
            .sortBy(identity_func)
            .map(lambda x: x + "_")
            .save_as_single_text_file((cfg.entity_ids / "identifiers.txt"))
        )

    if not (cfg.entity_ids / "metadata.txt").exists():
        prefixes = {"P", "Q", "L"}
        seen_prefixes = set()

        # open in bytes mode to use .tell to get the current byte position
        with open(str(cfg.entity_ids / "identifiers.txt"), "rb") as f, tqdm(
            total=(cfg.entity_ids / "identifiers.txt").stat().st_size,
            unit="B",
            unit_scale=True,
            desc="verifying entity ids",
        ) as pbar:
            n_ids = 0
            last_bytes = 0
            prev_id = ""
            for line in f:
                id = line.strip().decode()[:-1]
                if prev_id >= id:
                    raise ValueError(
                        f"Id must be unique and sorted, but found: {prev_id} and {id}"
                    )
                prev_id = id

                assert id[0] in prefixes and id[1:].isdigit(), id
                seen_prefixes.add(id[0])

                current_bytes = f.tell()
                pbar.update(current_bytes - last_bytes)
                last_bytes = current_bytes

                # verify the `is_entity_id` function
                assert is_entity_id(id), id

                n_ids += 1

        (cfg.entity_ids / "metadata.txt").write_text(
            f"""
seen id prefixes: {sorted(seen_prefixes)}
total number of ids: {n_ids}
""".strip()
        )

    return id_ds
