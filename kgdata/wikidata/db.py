from pathlib import Path
from typing import *

import gzip
from hugedict.misc import compress_custom, decompress_custom, identity
import requests

from hugedict.rocksdb import RocksDBDict
from kgdata.wikidata.models.qnode import QNode, QNodeLabel
from kgdata.wikidata.models.wdclass import WDClass
from kgdata.wikidata.models.wdproperty import WDProperty

V = TypeVar("V", bound=Union[QNode, WDClass, WDProperty])


class WDLocalDB(RocksDBDict[str, V]):
    def __init__(
        self,
        EntClass,
        dbfile: Union[Path, str],
        compression: bool,
        create_if_missing=True,
        read_only=False,
    ):
        super().__init__(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=decompress_custom(EntClass.deserialize)
            if compression
            else EntClass.deserialize,
            ser_value=compress_custom(EntClass.serialize)
            if compression
            else EntClass.serialize,
        )


class WDProxyDB(RocksDBDict[str, V]):
    def __init__(
        self,
        EntClass,
        dbfile: Union[Path, str],
        compression: bool,
        create_if_missing=True,
        read_only=False,
    ):
        super().__init__(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=decompress_custom(EntClass.deserialize)
            if compression
            else EntClass.deserialize,
            ser_value=compress_custom(EntClass.serialize)
            if compression
            else EntClass.serialize,
        )

        if not hasattr(EntClass, "from_qnode"):
            self.extract_ent_from_qnode = identity
        else:
            self.extract_ent_from_qnode = getattr(EntClass, "from_qnode")

    def __getitem__(self, key: str):
        item = self.db.get(key.encode())
        if item == b"\x00":
            raise KeyError(key)
        elif item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self.db.put(key.encode(), b"\x00")
                raise KeyError(key)
            else:
                ent = self.extract_ent_from_qnode(qnodes[key])
                self.db.put(key.encode(), self.ser_value(ent))
            return ent
        return self.deser_value(item)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        item = self.db.get(key.encode())
        if item == b"\x00":
            return False
        if item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self.db.put(key.encode(), b"\x00")
                return False

            ent = self.extract_ent_from_qnode(qnodes[key])
            self.db.put(key.encode(), self.ser_value(ent))
        return True

    def does_not_exist_locally(self, key):
        item = self.db.get(key.encode())
        return item == b"\x00"


def get_wikipedia_to_wikidata_db(
    dbpath: Union[Path, str],
) -> RocksDBDict[str, str]:
    return RocksDBDict(
        dbpath,
        create_if_missing=False,
        read_only=True,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=bytes.decode,
        ser_value=str.encode,
    )


def get_qnode_label_db(
    dbfile: Union[Path, str],
    compression: bool = False,
) -> RocksDBDict[str, QNodeLabel]:
    return RocksDBDict(
        dbfile,
        read_only=True,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=decompress_custom(QNodeLabel.deserialize)
        if compression
        else QNodeLabel.deserialize,
        ser_value=compress_custom(QNodeLabel.serialize)
        if compression
        else QNodeLabel.serialize,
    )


def get_qnode_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
    compression: bool = False,
    is_singleton: bool = False,
    cache_dict={},
) -> RocksDBDict[str, QNode]:
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(QNode, dbfile, compression, create_if_missing, read_only)
        else:
            db = WDLocalDB(QNode, dbfile, compression, create_if_missing, read_only)
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def get_wdclass_db(
    dbfile: str,
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
    compression: bool = False,
    is_singleton: bool = False,
    cache_dict={},
) -> RocksDBDict[str, WDClass]:
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(WDClass, dbfile, compression, create_if_missing, read_only)
        else:
            db = WDLocalDB(WDClass, dbfile, compression, create_if_missing, read_only)
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def get_wdprop_db(
    dbfile: str,
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
    is_singleton: bool = False,
    compression: bool = False,
    cache_dict={},
) -> RocksDBDict[str, WDProperty]:
    if not is_singleton or dbfile not in cache_dict:
        if proxy:
            db = WDProxyDB(
                WDProperty, dbfile, compression, create_if_missing, read_only
            )
        else:
            db = WDLocalDB(
                WDProperty, dbfile, compression, create_if_missing, read_only
            )
        if is_singleton:
            cache_dict[dbfile] = db
        return db
    return cache_dict[dbfile]


def query_wikidata_entities(
    qnode_ids: Union[Set[str], List[str]],
    endpoint: str = "https://www.wikidata.org/w/api.php",
) -> Dict[str, QNode]:
    assert len(qnode_ids) > 0, qnode_ids
    resp = requests.get(
        endpoint,
        params={
            "action": "wbgetentities",
            "ids": "|".join(qnode_ids),
            "format": "json",
        },
    )
    assert resp.status_code, resp
    data = resp.json()
    if "success" not in data:
        assert "error" in data, data
        # we have invalid entity id format, if we only query for one entity
        # we can tell it doesn't exist, otherwise, we don't know which entity are wrong
        if len(qnode_ids) == 1:
            return {}
        raise Exception(
            f"Invalid entity ID format. Don't know which entities are wrong. {qnode_ids}"
        )
    else:
        assert data.get("success", None) == 1, data
    qnodes = {}

    for qnode_id in qnode_ids:
        if "missing" in data["entities"][qnode_id]:
            continue
        qnode = QNode.from_wikidump(data["entities"][qnode_id])
        qnodes[qnode.id] = qnode
        if qnode.id != qnode_id:
            # redirection -- the best way is to update the redirection map
            origin_qnode = qnode.shallow_clone()
            origin_qnode.id = qnode_id
            qnodes[qnode_id] = origin_qnode
    return qnodes
