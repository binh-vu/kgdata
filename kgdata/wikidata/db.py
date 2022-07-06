from functools import partial
from pathlib import Path
from typing import *

from hugedict.types import HugeMutableMapping
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
import orjson
from hugedict.misc import zstd6_compress_custom, zstd_decompress_custom, identity
import requests

from hugedict.prelude import RocksDBDict, RocksDBOptions
from kgdata.wikidata.models import (
    WDClass,
    WDProperty,
    WDPropertyRanges,
    WDPropertyDomains,
)

V = TypeVar("V", WDEntity, WDClass, WDProperty, WDEntityLabel)


class WDProxyDB(RocksDBDict, HugeMutableMapping[str, V]):
    def __init__(
        self,
        EntClass,
        dbpath: Union[Path, str],
        dboptions: Optional[RocksDBOptions] = None,
        compression: bool = False,
        create_if_missing=True,
        readonly=False,
    ):
        super().__init__(
            path=str(dbpath),
            options=RocksDBOptions(create_if_missing=create_if_missing)
            if dboptions is None
            else dboptions,
            readonly=readonly,
            deser_key=partial(str, encoding="utf-8"),
            deser_value=zstd_decompress_custom(partial(deserialize, EntClass))
            if compression
            else partial(deserialize, EntClass),
            ser_value=zstd6_compress_custom(serialize) if compression else serialize,
        )

        if not hasattr(EntClass, "from_entity"):
            self.extract_ent_from_entity: Callable[[WDEntity], V] = identity
        else:
            self.extract_ent_from_entity: Callable[[WDEntity], V] = getattr(
                EntClass, "from_entity"
            )

    def __getitem__(self, key: str):
        item = self.get(key)
        if item == b"\x00":
            raise KeyError(key)
        elif item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self._put(key.encode(), b"\x00")
                raise KeyError(key)
            else:
                ent = self.extract_ent_from_entity(qnodes[key])
                self._put(key.encode(), self.ser_value(ent))

            return ent
        return self.deser_value(item)

    def get(self, key: str, default: Optional[V] = None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key):
        item = self.get(key)
        if item == b"\x00":
            return False
        if item is None:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self._put(key.encode(), b"\x00")
                return False

            ent = self.extract_ent_from_entity(qnodes[key])
            self._put(key.encode(), self.ser_value(ent))
        return True

    def does_not_exist_locally(self, key):
        item = self.get(key)
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
) -> HugeMutableMapping[str, WDClass]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
        bottommost_compression_type="zstd",
    )
    if proxy:
        db = WDProxyDB(
            WDClass,
            dbpath=dbfile,
            dboptions=dboptions,
            compression=False,
            readonly=read_only,
        )
    else:
        db = RocksDBDict(
            path=str(dbfile),
            options=dboptions,
            readonly=read_only,
            deser_key=partial(str, encoding="utf-8"),
            deser_value=partial(deserialize, WDClass),
            ser_value=serialize,
        )
    return db


def get_wdprop_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> HugeMutableMapping[str, WDProperty]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    if proxy:
        db = WDProxyDB(
            WDProperty,
            dbfile,
            dboptions=dboptions,
            compression=False,
            readonly=read_only,
        )
    else:
        db = RocksDBDict(
            path=str(dbfile),
            options=dboptions,
            readonly=read_only,
            deser_key=partial(str, encoding="utf-8"),
            deser_value=partial(deserialize, WDProperty),
            ser_value=serialize,
        )
    return db


def get_entity_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> HugeMutableMapping[str, WDEntity]:
    # no compression as we pre-compress the qnodes -- get much better compression
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="none",
    )
    if proxy:
        db: RocksDBDict[str, WDEntity] = WDProxyDB(
            WDEntity,
            dbfile,
            compression=True,
            readonly=read_only,
            dboptions=dboptions,
        )
    else:
        db = RocksDBDict(
            path=str(dbfile),
            options=dboptions,
            readonly=read_only,
            deser_key=partial(str, encoding="utf-8"),
            deser_value=zstd_decompress_custom(partial(deserialize, WDEntity)),
            ser_value=zstd6_compress_custom(serialize),
        )
    return db


def get_entity_redirection_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, str]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    return RocksDBDict(
        str(dbfile),
        readonly=read_only,
        options=dboptions,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(str, encoding="utf-8"),
        ser_value=str.encode,
    )


def get_entity_label_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, WDEntityLabel]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    return RocksDBDict(
        str(dbfile),
        readonly=read_only,
        options=dboptions,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(deserialize, WDEntityLabel),
        ser_value=serialize,
    )


def get_wp2wd_db(
    dbpath: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, str]:
    """Mapping from wikipedia article to wikidata id"""
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    return RocksDBDict(
        path=str(dbpath),
        options=dboptions,
        readonly=read_only,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(str, encoding="utf-8"),
        ser_value=str.encode,
    )


def get_wdprop_range_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, WDPropertyRanges]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    return RocksDBDict(
        str(dbfile),
        readonly=read_only,
        options=dboptions,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
    )


def get_wdprop_domain_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, WDPropertyDomains]:
    dboptions = RocksDBOptions(
        create_if_missing=create_if_missing,
        compression_type="lz4",
    )
    return RocksDBDict(
        str(dbfile),
        readonly=read_only,
        options=dboptions,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
    )
