from typing import *

import gzip
import requests

from sm.misc.big_dict import RocksDBStore
from kgdata.wikidata.models.qnode import QNode
from kgdata.wikidata.models.wdclass import WDClass
from kgdata.wikidata.models.wdproperty import WDProperty

V = TypeVar('V', bound=Union[QNode, WDClass, WDProperty])


def ent_gzip_deserialize(EntClass):
    def deserialize(value):
        return EntClass.deserialize(gzip.decompress(value))
    return deserialize


class WDLocalDB(RocksDBStore[str, V]):
    def __init__(self, EntClass, dbfile: str, compression: bool, create_if_missing=True, read_only=False):
        super().__init__(
            dbfile,
            deserialize=ent_gzip_deserialize(EntClass) if compression else EntClass.deserialize,
            create_if_missing=create_if_missing, 
            read_only=read_only)
        self.EntClass = EntClass

    def __setitem__(self, key, value):
        self.db.put(key.encode(), value.serialize())

    def deserialize(self, value):
        return self.EntClass.deserialize(value)


class WDProxyDB(RocksDBStore[str, V]):
    def __init__(self, EntClass, dbfile: str, compression: bool, create_if_missing=True, read_only=False):
        super().__init__(
            dbfile,
            deserialize=ent_gzip_deserialize(EntClass) if compression else EntClass.deserialize,
            create_if_missing=create_if_missing, 
            read_only=read_only)

        self.compression = compression
        self.EntClass = EntClass

        if not hasattr(self.EntClass, "from_qnode"):
            self.extract_ent_from_qnode = lambda x: x
        else:
            self.extract_ent_from_qnode = getattr(self.EntClass, "from_qnode")

    def __getitem__(self, key):
        item = self.db.get(key.encode())
        if item == b'\x00':
            raise KeyError(key)
        elif item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self.db.put(key.encode(), b'\x00')
                raise KeyError(key)
            else:
                ent = self.extract_ent_from_qnode(qnodes[key])
                value = ent.serialize()
                if self.compression:
                    value = gzip.compress(value)
                self.db.put(key.encode(), value)
            return ent
        return self.deserialize(item)

    def __setitem__(self, key, value):
        value = value.serialize()
        if self.compression:
            value = gzip.compress(value)
        self.db.put(key.encode(), value)

    def __contains__(self, key):
        item = self.db.get(key.encode())
        if item == b'\x00':
            return False
        if item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self.db.put(key.encode(), b'\x00')
                return False

            ent = self.extract_ent_from_qnode(qnodes[key])
            value = ent.serialize()
            if self.compression:
                value = gzip.compress(value)
            self.db.put(key.encode(), value)
        return True

    def does_not_exist(self, key):
        item = self.db.get(key.encode())
        return item == b'\x00'

    def get(self, key: str, default=None):
        item = self.db.get(key.encode())
        if item == b'\x00':
            return default
        elif item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self.db.put(key.encode(), b'\x00')
                return default
            else:
                ent = self.extract_ent_from_qnode(qnodes[key])
                value = ent.serialize()
                if self.compression:
                    value = gzip.compress(value)
                self.db.put(key.encode(), value)
            return ent
        return self.deserialize(item)


def get_qnode_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool = False, 
                 compression: bool = False, is_singleton: bool = False,
                 cache_dict={}):
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(QNode, dbfile, compression, create_if_missing, read_only)
        else:
            db = WDLocalDB(QNode, dbfile, compression, create_if_missing, read_only)
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def get_wdclass_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool = False,
                   compression: bool = False,
                   is_singleton: bool = False, cache_dict={}):
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(WDClass, dbfile, compression, create_if_missing, read_only)
        else:
            db = WDLocalDB(WDClass, dbfile, compression, create_if_missing, read_only)
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def get_wdprop_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool = False, is_singleton: bool = False,
                  compression: bool = False, cache_dict={}):
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(WDProperty, dbfile, compression, create_if_missing, read_only)
        else:
            db = WDLocalDB(WDProperty, dbfile, compression, create_if_missing, read_only)
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def query_wikidata_entities(qnode_ids: Union[Set[str], List[str]]) -> Dict[str, QNode]:
    assert len(qnode_ids) > 0, qnode_ids
    resp = requests.get("https://www.wikidata.org/w/api.php", params={
        "action": "wbgetentities",
        "ids": "|".join(qnode_ids),
        "format": "json"
    })
    assert resp.status_code, resp
    data = resp.json()
    if 'success' not in data:
        assert 'error' in data, data
        # we have invalid entity id format, if we only query for one entity
        # we can tell it doesn't exist, otherwise, we don't know which entity are wrong
        if len(qnode_ids) == 1:
            return {}
        raise Exception(f"Invalid entity ID format. Don't know which entities are wrong. {qnode_ids}")
    else:
        assert data.get('success', None) == 1, data
    qnodes = {}

    for qnode_id in qnode_ids:
        if 'missing' in data['entities'][qnode_id]:
            continue
        qnode = QNode.from_wikidump(data['entities'][qnode_id])
        qnodes[qnode.id] = qnode
        if qnode.id != qnode_id:
            # redirection -- the best way is to update the redirection map
            origin_qnode = qnode.shallow_clone()
            origin_qnode.id = qnode_id
            qnodes[qnode_id] = origin_qnode
    return qnodes
