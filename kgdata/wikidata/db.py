"""Wikidata embedded key-value databases."""

from __future__ import annotations

from functools import partial
import functools
from pathlib import Path
from typing import (
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import orjson
import requests
import serde.jl as jl
from hugedict.misc import identity
from hugedict.prelude import (
    CacheDict,
    RocksDBCompressionOptions,
    RocksDBDict,
    RocksDBOptions,
)
from hugedict.types import HugeMutableMapping

from kgdata.wikidata.extra_ent_db import EntAttr, get_entity_attr_db
from kgdata.wikidata.models import (
    WDClass,
    WDProperty,
    WDPropertyDomains,
    WDPropertyRanges,
)
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel

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
    """Mapping from wikipedia article's title to wikidata id"""
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


class WikidataDB:
    """Helper class to make it easier to load all Wikidata databases stored in a directory.
    The Wikidata database is expected to be stored in the directory under specific names.

    It makes use of the `functools.cached_property` decorator to cache the database objects
    as attributes of the class after the first access.
    """

    instance = None

    def __init__(self, database_dir: Union[str, Path]):
        self.database_dir = Path(database_dir)

    @functools.cached_property
    def wdentities(self):
        return get_entity_db(self.database_dir / "wdentities.db", read_only=True)

    @functools.cached_property
    def wdclasses(self):
        wdclasses = get_wdclass_db(self.database_dir / "wdclasses.db", read_only=True)
        if (self.database_dir / "wdclasses.fixed.jl").exists():
            wdclasses = wdclasses.cache()
            assert isinstance(wdclasses, CacheDict)
            for record in jl.deser(self.database_dir / "wdclasses.fixed.jl"):
                cls = WDClass.from_dict(record)
                wdclasses._cache[cls.id] = cls
        return wdclasses

    @functools.cached_property
    def wdprops(self):
        return get_wdprop_db(self.database_dir / "wdprops.db", read_only=True)

    @functools.cached_property
    def wdprop_domains(self):
        return get_wdprop_domain_db(
            self.database_dir / "wdprop_domains.db", read_only=True
        )

    @functools.cached_property
    def wdprop_ranges(self):
        return get_wdprop_range_db(
            self.database_dir / "wdprop_ranges.db", read_only=True
        )

    @functools.cached_property
    def wdredirections(self):
        return get_entity_redirection_db(
            self.database_dir / "wdentity_redirections.db", read_only=True
        )

    @functools.cached_property
    def wp2wd(self):
        return get_wp2wd_db(self.database_dir / "wd2wp.db", read_only=True)

    @functools.cached_property
    def wdpagerank(self):
        return get_entity_attr_db(
            self.database_dir / "wdentities_attr.db",
            "pagerank",
            read_only=True,
        )

    @overload
    def wdattr(
        self, attr: Literal["aliases", "instanceof"]
    ) -> HugeMutableMapping[str, list[str]]:
        ...

    @overload
    def wdattr(
        self, attr: Literal["label", "description"]
    ) -> HugeMutableMapping[str, str]:
        ...

    def wdattr(self, attr: EntAttr):
        return get_entity_attr_db(
            self.database_dir / "wdentities_attr.db",
            attr,
            read_only=True,
        )

    @staticmethod
    def init(database_dir: Union[str, Path]) -> "WikidataDB":
        assert WikidataDB.instance is None
        WikidataDB.instance = WikidataDB(database_dir)
        return WikidataDB.instance

    @staticmethod
    def get_instance() -> "WikidataDB":
        assert WikidataDB.instance is not None
        return WikidataDB.instance
