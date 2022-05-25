from functools import partial
from pathlib import Path
from typing import *
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
import orjson
import rocksdb
import gzip
from hugedict.misc import zstd6_compress_custom, zstd_decompress_custom, identity
import requests

from hugedict.rocksdb import RocksDBDict
from kgdata.wikidata.models import WDClass, WDProperty

V = TypeVar("V", WDEntity, WDClass, WDProperty, WDEntityLabel)
CompressionType = rocksdb.CompressionType  # type: ignore


class WDProxyDB(RocksDBDict[str, V]):
    def __init__(
        self,
        EntClass,
        dbfile: Union[Path, str],
        compression: bool,
        create_if_missing=True,
        read_only=False,
        db_options: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=zstd_decompress_custom(partial(deserialize, EntClass))
            if compression
            else partial(deserialize, EntClass),
            ser_value=zstd6_compress_custom(serialize) if compression else serialize,
            db_options=db_options,
        )

        if not hasattr(EntClass, "from_entity"):
            self.extract_ent_from_entity: Callable[[WDEntity], V] = identity
        else:
            self.extract_ent_from_entity: Callable[[WDEntity], V] = getattr(
                EntClass, "from_entity"
            )

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
                ent = self.extract_ent_from_entity(qnodes[key])
                self.db.put(key.encode(), self.ser_value(ent))
            return ent
        return self.deser_value(item)

    def get(self, key: str, default: Optional[V] = None):
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

            ent = self.extract_ent_from_entity(qnodes[key])
            self.db.put(key.encode(), self.ser_value(ent))
        return True

    def does_not_exist_locally(self, key):
        item = self.db.get(key.encode())
        return item == b"\x00"


def query_wikidata_entities(
    qnode_ids: Union[Set[str], List[str]],
    endpoint: str = "https://www.wikidata.org/w/api.php",
) -> Dict[str, WDEntity]:
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
        qnode = WDEntity.from_wikidump(data["entities"][qnode_id])
        qnodes[qnode.id] = qnode
        if qnode.id != qnode_id:
            # redirection -- the best way is to update the redirection map
            origin_qnode = qnode.shallow_clone()
            origin_qnode.id = qnode_id
            qnodes[qnode_id] = origin_qnode
    return qnodes


def serialize(ent: V) -> bytes:
    return orjson.dumps(ent.to_dict())


def deserialize(cls: Type[V], data: Union[str, bytes]) -> V:
    return cls.from_dict(orjson.loads(data))  # type: ignore


def get_wdclass_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> RocksDBDict[str, WDClass]:
    db_options = {
        "compression": CompressionType.lz4_compression,
        "bottommost_compression": CompressionType.zstd_compression,
    }
    if proxy:
        db = WDProxyDB(
            WDClass,
            dbfile,
            compression=False,
            create_if_missing=create_if_missing,
            read_only=read_only,
            db_options=db_options,
        )
    else:
        db = RocksDBDict(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=partial(deserialize, WDClass),
            ser_value=serialize,
            db_options=db_options,
        )
    return db


def get_wdprop_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> RocksDBDict[str, WDProperty]:
    db_options = {"compression": CompressionType.lz4_compression}
    if proxy:
        db = WDProxyDB(
            WDProperty,
            dbfile,
            compression=False,
            create_if_missing=create_if_missing,
            read_only=read_only,
            db_options=db_options,
        )
    else:
        db = RocksDBDict(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=partial(deserialize, WDProperty),
            ser_value=serialize,
            db_options=db_options,
        )
    return db


def get_entity_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> RocksDBDict[str, WDEntity]:
    # no compression as we pre-compress the qnodes -- get much better compression
    db_options = {"compression": CompressionType.no_compression}
    if proxy:
        db: RocksDBDict[str, WDEntity] = WDProxyDB(
            WDEntity,
            dbfile,
            compression=True,
            create_if_missing=create_if_missing,
            read_only=read_only,
            db_options=db_options,
        )
    else:
        db = RocksDBDict(
            dbpath=dbfile,
            create_if_missing=create_if_missing,
            read_only=read_only,
            deser_key=bytes.decode,
            ser_key=str.encode,
            deser_value=zstd_decompress_custom(partial(deserialize, WDEntity)),
            ser_value=zstd6_compress_custom(serialize),
            db_options=db_options,
        )
    return db


def get_entity_redirection_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> RocksDBDict[str, str]:
    db_options = {
        "compression": CompressionType.lz4_compression,
    }
    return RocksDBDict(
        dbfile,
        create_if_missing=create_if_missing,
        read_only=read_only,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=bytes.decode,
        ser_value=str.encode,
        db_options=db_options,
    )


def get_entity_label_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> RocksDBDict[str, WDEntityLabel]:
    db_options = {
        "compression": CompressionType.lz4_compression,
    }
    return RocksDBDict(
        dbfile,
        create_if_missing=create_if_missing,
        read_only=read_only,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=partial(deserialize, WDEntityLabel),
        ser_value=serialize,
        db_options=db_options,
    )


def get_wp2wd_db(
    dbpath: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> RocksDBDict[str, str]:
    """Mapping from wikipedia article to wikidata id"""
    db_options = {"compression": CompressionType.lz4_compression}
    return RocksDBDict(
        dbpath,
        create_if_missing=create_if_missing,
        read_only=read_only,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=bytes.decode,
        ser_value=str.encode,
        db_options=db_options,
    )


def get_wdprop_range_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> RocksDBDict[str, Dict[str, int]]:
    db_options = {
        "compression": CompressionType.lz4_compression,
    }
    return RocksDBDict(
        dbfile,
        create_if_missing=create_if_missing,
        read_only=read_only,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
        db_options=db_options,
    )


def get_wdprop_domain_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> RocksDBDict[str, Dict[str, int]]:
    db_options = {
        "compression": CompressionType.lz4_compression,
    }
    return RocksDBDict(
        dbfile,
        create_if_missing=create_if_missing,
        read_only=read_only,
        deser_key=bytes.decode,
        ser_key=str.encode,
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
        db_options=db_options,
    )
