import glob
import os
from dataclasses import asdict
from multiprocessing import Pool

import orjson
import redis
from tqdm.auto import tqdm

from kgdata.config import WIKIDATA_REDIS, DBPEDIA_REDIS, WIKIDATA_ONTOLOGY_REDIS
from kgdata.dbpedia.prelude import Table
from kgdata.wikidata.prelude import QNode, WDClass, WDProperty
from sm_unk.prelude import M
from sm_unk.misc import RedisStore


class WikiDataStore(RedisStore[str, QNode]):
    def __init__(self):
        super().__init__(WIKIDATA_REDIS)

    def deserialize(self, value: str):
        return QNode.deserialize(value)


class DBPediaTableStore(RedisStore[str, Table]):
    def __init__(self):
        super().__init__(DBPEDIA_REDIS)

    def deserialize(self, value: str):
        return Table.deser_str(value)


class WDClassStore(RedisStore[str, WDClass]):
    def __init__(self):
        super().__init__(WIKIDATA_ONTOLOGY_REDIS)

    def deserialize(self, value: str):
        item = WDClass(**orjson.loads(value))
        item.parents_closure = set(item.parents_closure)
        return item


class WDPropertyStore(RedisStore[str, WDProperty]):
    def __init__(self):
        super().__init__(WIKIDATA_ONTOLOGY_REDIS)

    def values(self):
        for k in self.redis.scan_iter("P*"):
            yield self[k.decode()]

    def deserialize(self, value: str):
        item = WDProperty(**orjson.loads(value))
        item.parents_closure = set(item.parents_closure)
        return item


class DataLoader:
    def __init__(self, is_dbpedia) -> None:
        if is_dbpedia:
            self.Store = DBPediaTableStore
        else:
            self.Store = WikiDataStore

    def load_fn_pair(self, infile):
        store = self.Store.get_instance()
        for key, val in M.deserialize_key_val_lines(infile):
            store[key] = val
        return True


def load_wd_ontology():
    store = WDClassStore.get_instance()
    for c in tqdm(WDClass.from_file(load_parent_closure=True).values()):
        store[c.id] = orjson.dumps(c, option=orjson.OPT_SERIALIZE_DATACLASS, default=list)

    store = WDPropertyStore.get_instance()
    for p in tqdm(WDProperty.from_file(load_parent_closure=True).values()):
        store[p.id] = orjson.dumps(p, option=orjson.OPT_SERIALIZE_DATACLASS, default=list)


if __name__ == "__main__":
    from sm_unk.config import HOME_DIR

    # load_wd_ontology()
    # exit(0)

    # pattern = "dbpedia/tables_en/step_5_populated_relational_tables/step_1/*.gz"
    # pattern = "wikidata/ontology/step_1/*.gz"
    # pattern = "wikitable2wikidata/ethiopia_tables/qnodes/*.gz"
    # pattern = "wikitable2wikidata/norvatis_tables/qnodes/qnodes_2/*.gz"
    # pattern = "wikitable2wikidata/semtab2020_subset/qnodes/qnodes_3/*.gz"
    pattern = "wikitable2wikidata/semtab2020_subset/qnodes/qnode_classes/*.gz"
    # pattern = "wikitable2wikidata/semtab2020/qnodes/qnodes_3/*.gz"
    # pattern = "wikitable2wikidata/semtab2020/qnodes/qnode_classes/*.gz"
    # pattern = "wikitable2wikidata/500tables/qnodes/qnodes_3/*.gz"
    # pattern = "wikitable2wikidata/250tables/qnodes/qnodes_3/*.gz"
    # pattern = "wikitable2wikidata/250tables/qnodes/qnode_classes/*.gz"
    # pattern = "wikitable2wikidata/250tables_fixed/qnodes/qnodes_3/*.gz"
    is_dbpedia = False

    # pattern = "wikitable2wikidata/tables/s0_*/*.gz"
    # is_dbpedia = True

    infiles = sorted(glob.glob(os.path.join(HOME_DIR, pattern)))
    load_fn = DataLoader(is_dbpedia=is_dbpedia).load_fn_pair
    # load_fn(infiles[0])
    with Pool() as pool:
        for x in tqdm(pool.imap_unordered(load_fn, infiles), total=len(infiles)):
            pass
