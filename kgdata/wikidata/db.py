"""Wikidata embedded key-value databases."""

from __future__ import annotations

from functools import cached_property, partial
from pathlib import Path
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
    overload,
)

import orjson
import requests
import serde.jl as jl
from hugedict.prelude import CacheDict, RocksDBDict
from hugedict.types import HugeMutableMapping
from kgdata.db import (
    GenericDB,
    deser_from_dict,
    deser_from_tuple,
    get_rocksdb,
    large_dbopts,
    make_get_rocksdb,
    medium_dbopts,
    pack_float,
    pack_int,
    ser_to_dict,
    ser_to_tuple,
    small_dbopts,
    unpack_float,
    unpack_int,
)
from kgdata.models.entity import EntityOutLinks
from kgdata.wikidata.datasets.mention_to_entities import MentionToEntities
from kgdata.wikidata.extra_ent_db import EntAttr, get_entity_attr_db
from kgdata.wikidata.models import WDClass, WDProperty
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdentitylabel import WDEntityLabel
from kgdata.wikidata.models.wdentitylink import WDEntityWikiLink
from kgdata.wikidata.models.wdentitymetadata import WDEntityMetadata
from sm.misc.funcs import import_func

V = TypeVar("V", WDEntity, WDClass, WDProperty, WDEntityLabel, WDEntityWikiLink)


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
    assert (
        len(qnode_ids) > 0 and len(qnode_ids) <= 50
    ), "The query limit is 50, but get: %d" % len(qnode_ids)
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


WrappableClass = TypeVar("WrappableClass", WDClass, WDProperty, WDEntity)


class proxy_wrapper(Generic[WrappableClass]):
    def __init__(
        self,
        clz: type[WrappableClass],
        dbopts: dict | None = None,
    ):
        self.cls = clz
        self.dbopts = dbopts

    def __call__(
        self,
        dbfile: Union[Path, str],
        *,
        create_if_missing: bool = True,
        read_only: bool = False,
        proxy: bool = False,
    ) -> HugeMutableMapping[str, WrappableClass]:
        if proxy:
            constructor = WDProxyDB
            create_if_missing = True
        else:
            constructor = RocksDBDict

        db = get_rocksdb(
            dbfile,
            ser_value=ser_to_dict,
            deser_value=partial(deser_from_dict, self.cls),
            cls=constructor,
            create_if_missing=create_if_missing,
            read_only=read_only,
            dbopts=self.dbopts,
        )
        if proxy:
            if hasattr(self.cls, "from_entity"):
                return cast(WDProxyDB, db).set_extract_ent_from_entity(self.cls.from_entity)  # type: ignore
            return cast(WDProxyDB, db).set_extract_ent_from_entity(lambda x: x)
        return db  # type: ignore


get_class_db = proxy_wrapper(clz=WDClass, dbopts=medium_dbopts)
get_prop_db = proxy_wrapper(clz=WDProperty, dbopts=small_dbopts)
get_entity_db = proxy_wrapper(clz=WDEntity, dbopts=large_dbopts)
get_entity_metadata_db = make_get_rocksdb(
    deser_value=partial(deser_from_tuple, WDEntityMetadata),
    ser_value=ser_to_tuple,
    dbopts=large_dbopts,
    version="2",
)
get_entity_redirection_db = make_get_rocksdb(
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)
get_entity_label_db = make_get_rocksdb(
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)
get_entity_type_db: make_get_rocksdb[list[str]] = make_get_rocksdb(
    deser_value=orjson.loads,
    ser_value=orjson.dumps,
    dbopts=small_dbopts,
)
get_entity_pagerank_db = make_get_rocksdb(
    deser_value=unpack_float,
    ser_value=pack_float,
    dbopts={"compression_type": "none"},
)
get_entity_outlinks_db = make_get_rocksdb(
    deser_value=partial(deser_from_dict, EntityOutLinks),
    ser_value=ser_to_dict,
    dbopts=small_dbopts,
)
get_wp2wd_db = make_get_rocksdb(
    deser_value=partial(str, encoding="utf-8"),
    ser_value=str.encode,
    dbopts=small_dbopts,
)
get_ontcount_db = make_get_rocksdb(
    deser_value=unpack_int,
    ser_value=pack_int,
    dbopts={"compression_type": "none"},
)
get_prop_range_db = make_get_rocksdb(
    deser_value=orjson.loads,
    ser_value=orjson.dumps,
    dbopts=small_dbopts,
)
get_prop_domain_db = make_get_rocksdb(
    deser_value=orjson.loads,
    ser_value=orjson.dumps,
    dbopts=small_dbopts,
)
get_mention_to_entities_db: make_get_rocksdb[list[tuple[str, tuple[int, int]]]] = (
    make_get_rocksdb(
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
        dbopts=small_dbopts,
    )
)
get_norm_mentions_db: make_get_rocksdb[list[tuple[str, tuple[int, int]]]] = (
    make_get_rocksdb(
        deser_value=orjson.loads,
        ser_value=orjson.dumps,
        dbopts=small_dbopts,
    )
)
get_label2ids_db = partial(
    get_rocksdb,
    deser_value=orjson.loads,
    ser_value=orjson.dumps,
    dbopts=small_dbopts,
)


class WikidataDB(GenericDB):
    """Helper class to make it easier to load all Wikidata databases stored in a directory.
    The Wikidata database is expected to be stored in the directory under specific names.

    It makes use of the `functools.cached_property` decorator to cache the database objects
    as attributes of the class after the first access.
    """

    instance = None

    def __init__(self, database_dir: Path | str, read_only: bool = True):
        self.database_dir = Path(database_dir)
        self.read_only = read_only

    @cached_property
    def entities(self):
        return get_entity_db(
            self.database_dir / "entities.db", read_only=self.read_only
        )

    @cached_property
    def entity_metadata(self):
        return get_entity_metadata_db(
            self.database_dir / "entity_metadata.db", read_only=self.read_only
        )

    @cached_property
    def entity_labels(self):
        return get_entity_label_db(
            self.database_dir / "entity_labels.db", read_only=self.read_only
        )

    @cached_property
    def entity_types(self):
        return get_entity_type_db(
            self.database_dir / "entity_types.db", read_only=self.read_only
        )

    @cached_property
    def classes(self):
        wdclasses = get_class_db(
            self.database_dir / "classes.db", read_only=self.read_only
        )
        if (self.database_dir / "classes.fixed.jl").exists():
            wdclasses = wdclasses.cache()
            assert isinstance(wdclasses, CacheDict)
            for record in jl.deser(self.database_dir / "classes.fixed.jl"):
                cls = WDClass.from_dict(record)
                wdclasses._cache[cls.id] = cls
        return wdclasses

    @cached_property
    def props(self):
        wdprops = get_prop_db(self.database_dir / "props.db", read_only=self.read_only)
        if (self.database_dir / "props.fixed.jl").exists():
            wdprops = wdprops.cache()
            assert isinstance(wdprops, CacheDict)
            for record in jl.deser(self.database_dir / "props.fixed.jl"):
                prop = WDProperty.from_dict(record)
                wdprops._cache[prop.id] = prop
        return wdprops

    @cached_property
    def ontcount(self):
        return get_ontcount_db(
            self.database_dir / "ontcount.db", read_only=self.read_only
        )

    @cached_property
    def prop_domains(self):
        return get_prop_domain_db(
            self.database_dir / "prop_domains.db", read_only=self.read_only
        )

    @cached_property
    def prop_ranges(self):
        return get_prop_range_db(
            self.database_dir / "prop_ranges.db", read_only=self.read_only
        )

    @cached_property
    def entity_redirections(self):
        return get_entity_redirection_db(
            self.database_dir / "entity_redirections.db", read_only=self.read_only
        )

    @cached_property
    def wp2wd(self):
        return get_wp2wd_db(self.database_dir / "wp2wd.db", read_only=self.read_only)

    @cached_property
    def entity_pagerank(self):
        return get_entity_pagerank_db(
            self.database_dir / "entity_pagerank.db", read_only=self.read_only
        )

    @cached_property
    def entity_outlinks(self):
        return get_entity_outlinks_db(
            self.database_dir / "entity_outlinks.db", read_only=self.read_only
        )

    @cached_property
    def mention_to_entities(self):
        return get_mention_to_entities_db(
            self.database_dir / "mention_to_entities.db", read_only=self.read_only
        )

    @cached_property
    def norm_mentions(self):
        return get_norm_mentions_db(
            self.database_dir / "norm_mentions.db", read_only=self.read_only
        )

    @cached_property
    def label2ids(self):
        return get_label2ids_db(
            self.database_dir / "label2ids.db", read_only=self.read_only
        )

    @overload
    def attr(
        self, attr: Literal["aliases", "instanceof"]
    ) -> HugeMutableMapping[str, list[str]]: ...

    @overload
    def attr(
        self, attr: Literal["label", "description"]
    ) -> HugeMutableMapping[str, str]: ...

    def attr(self, attr: EntAttr):
        return get_entity_attr_db(
            self.database_dir / "entities_attr.db",
            attr,
            read_only=self.read_only,
        )

    @staticmethod
    def init(database_dir: Union[str, Path]) -> "WikidataDB":
        if WikidataDB.instance is not None:
            assert WikidataDB.instance.database_dir == Path(database_dir)
        else:
            WikidataDB.instance = WikidataDB(database_dir)
        return WikidataDB.instance

    @staticmethod
    def get_instance() -> "WikidataDB":
        assert WikidataDB.instance is not None
        return WikidataDB.instance


if __name__ == "__main__":
    import click

    @click.command()
    @click.option("-d", "--data-dir", required=True, help="database directory")
    @click.option("-n", "--dbname", required=True, help="database name")
    @click.option("-f", "--format", default="", help="function to format the value")
    @click.argument("keys", nargs=-1)
    def cli(data_dir: str, dbname: str, format: str, keys: list[str]):
        if format == "":
            fmt = str
        else:
            fmt1 = import_func(format)
            fmt = lambda x: v.decode() if isinstance((v := fmt1(x)), bytes) else v

        db = getattr(WikidataDB(data_dir), dbname)
        if len(keys) > 0:
            for k in keys:
                print("key:", k)
                print("value:", fmt(db[k]))
                print("")
        else:
            for k in db.keys():
                print("key:", k)
                print("value:", fmt(db[k]))
                break

    cli()
