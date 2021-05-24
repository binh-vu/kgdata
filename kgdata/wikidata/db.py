from typing import *

import requests

from kgdata.misc.remote_dict import RocksDBStore
from kgdata.wikidata.models.qnode import QNode
from kgdata.wikidata.models.wdclass import WDClass
from kgdata.wikidata.models.wdproperty import WDProperty

V = TypeVar('V', bound=Union[QNode, WDClass, WDProperty])


class WDLocalDB(RocksDBStore[str, V]):
    def __init__(self, EntClass, dbfile: str, create_if_missing=True, read_only=False):
        super().__init__(dbfile, create_if_missing, read_only)
        self.EntClass = EntClass

    def __setitem__(self, key, value):
        self.db.put(key.encode(), value.serialize())

    def deserialize(self, value):
        return self.EntClass.deserialize(value)


class WDProxyDB(RocksDBStore[str, V]):
    def __init__(self, EntClass, dbfile: str, create_if_missing=True, read_only=False):
        super().__init__(dbfile, create_if_missing, read_only)
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
                self.db.put(key.encode(), ent.serialize())
            return ent
        return self.deserialize(item)

    def __setitem__(self, key, value):
        self.db.put(key.encode(), value.serialize())

    def __contains__(self, key):
        return self.db.get(key.encode()) != b'\x00'

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
                self.db.put(key.encode(), ent.serialize())
            return ent
        return self.deserialize(item)

    def deserialize(self, value):
        return self.EntClass.deserialize(value)


def get_qnode_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool=False):
    if proxy:
        return WDProxyDB(QNode, dbfile, create_if_missing, read_only)
    return WDLocalDB(QNode, dbfile, create_if_missing, read_only)


def get_wdclass_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool=False):
    if proxy:
        return WDProxyDB(WDClass, dbfile, create_if_missing, read_only)
    return WDLocalDB(WDClass, dbfile, create_if_missing, read_only)


def get_wdprop_db(dbfile: str, create_if_missing=True, read_only=False, proxy: bool=False):
    if proxy:
        return WDProxyDB(WDProperty, dbfile, create_if_missing, read_only)
    return WDLocalDB(WDProperty, dbfile, create_if_missing, read_only)


def query_wikidata_entities(qnode_ids: Union[Set[str], List[str]]) -> Dict[str, QNode]:
    resp = requests.get("https://www.wikidata.org/w/api.php", params={
        "action": "wbgetentities",
        "ids": "|".join(qnode_ids),
        "format": "json"
    })
    assert resp.status_code, resp
    data = resp.json()
    assert data['success'] == 1
    qnodes = {}

    for qnode_id in qnode_ids:
        if 'missing' in data['entities'][qnode_id]:
            continue
        qnodes[qnode_id] = QNode.from_wikidump(data['entities'][qnode_id])
    return qnodes
