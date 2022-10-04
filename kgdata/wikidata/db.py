"""Wikidata embedded key-value databases."""

from functools import partial
from pathlib import Path
from typing import (
    Dict,
    Literal,
    Union,
    TypeVar,
    Optional,
    Callable,
    Set,
    List,
    Type,
    cast,
)

from hugedict.types import HugeMutableMapping
from kgdata.wikidata.datasets.entity_metadata import (
    convert_to_entity_metadata,
    deser_entity_metadata,
    ser_entity_metadata,
)
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
from kgdata.wikidata.models.wdentitymetadata import WDEntityMetadata
import orjson
from hugedict.misc import zstd6_compress_custom, zstd_decompress_custom, identity
import requests

from hugedict.prelude import RocksDBDict, RocksDBOptions, RocksDBCompressionOptions
from kgdata.wikidata.models import (
    WDClass,
    WDProperty,
    WDPropertyRanges,
    WDPropertyDomains,
)

V = TypeVar("V", WDEntity, WDClass, WDProperty, WDEntityLabel)


class WDProxyDB(RocksDBDict, HugeMutableMapping[str, V]):
    def set_extract_ent_from_entity(self, func: Callable[[WDEntity], V]):
        self.extract_ent_from_entity = func
        return self

    def __getitem__(self, key: str):
        try:
            item = self._get(key.encode())
        except KeyError:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self._put(key.encode(), b"\x00")
                raise KeyError(key)
            else:
                ent = self.extract_ent_from_entity(qnodes[key])
                self._put(key.encode(), self.ser_value(ent))
            return ent

        if item == b"\x00":
            raise KeyError(key)
        return self.deser_value(item)  # type: ignore

    def get(self, key: str, default: Optional[V] = None):
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str):
        try:
            item = self._get(key.encode())
        except KeyError:
            qnodes = query_wikidata_entities([key])
            if len(qnodes) == 0:
                # no entity
                self._put(key.encode(), b"\x00")
                return False

            ent = self.extract_ent_from_entity(qnodes[key])
            self._put(key.encode(), self.ser_value(ent))
            return True

        if item == b"\x00":
            return False

        return True

    def does_not_exist_locally(self, key: str):
        try:
            item = self._get(key.encode())
        except KeyError:
            return True
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
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

    if proxy:
        constructor = WDProxyDB
        create_if_missing = True
    else:
        constructor = RocksDBDict

    db = constructor(
        path=str(dbfile),
        options=RocksDBOptions(
            create_if_missing=create_if_missing,
            compression_type="lz4",
            bottommost_compression_type="zstd",
        ),
        readonly=read_only,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(deserialize, WDClass),
        ser_value=serialize,
    )

    if proxy:
        return cast(WDProxyDB, db).set_extract_ent_from_entity(WDClass.from_entity)
    return db


def get_wdprop_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> HugeMutableMapping[str, WDProperty]:
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

    if proxy:
        constructor = WDProxyDB
        create_if_missing = True
    else:
        constructor = RocksDBDict

    db = constructor(
        path=str(dbfile),
        options=RocksDBOptions(
            create_if_missing=create_if_missing,
            compression_type="lz4",
        ),
        readonly=read_only,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(deserialize, WDProperty),
        ser_value=serialize,
    )

    if proxy:
        return cast(WDProxyDB, db).set_extract_ent_from_entity(WDProperty.from_entity)
    return db


def get_entity_db(
    dbfile: Union[Path, str],
    create_if_missing=True,
    read_only=False,
    proxy: bool = False,
) -> HugeMutableMapping[str, WDEntity]:
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "2", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("2")

    if proxy:
        constructor = WDProxyDB
        create_if_missing = True
    else:
        constructor = RocksDBDict

    db = constructor(
        path=str(dbfile),
        options=RocksDBOptions(
            create_if_missing=create_if_missing,
            compression_type="zstd",
            compression_opts=RocksDBCompressionOptions(
                window_bits=-14, level=6, strategy=0, max_dict_bytes=16 * 1024
            ),
            zstd_max_train_bytes=100 * 16 * 1024,
        ),
        readonly=read_only,
        deser_key=partial(str, encoding="utf-8"),
        deser_value=partial(deserialize, WDEntity),
        ser_value=serialize,
    )

    if proxy:
        return cast(WDProxyDB, db).set_extract_ent_from_entity(identity)

    return db


def get_entity_redirection_db(
    dbfile: Union[Path, str],
    create_if_missing=False,
    read_only=True,
) -> HugeMutableMapping[str, str]:
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

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
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

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
    version = Path(dbpath) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

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
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

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
    version = Path(dbfile) / "_VERSION"
    if version.exists():
        ver = version.read_text()
        assert ver.strip() == "1", ver
    else:
        version.parent.mkdir(parents=True, exist_ok=True)
        version.write_text("1")

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
