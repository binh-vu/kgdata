from typing import TypeVar, Dict, Union

import rocksdb

"""
Provide a in-memory database for storing huge dictionary. Mainly used for development.
"""

K = TypeVar('K')
V = TypeVar('V')


class RocksDBStore(Dict[K, V]):
    def __init__(self, dbfile: str, create_if_missing=True, read_only=False):
        self.db = rocksdb.DB(str(dbfile), rocksdb.Options(create_if_missing=create_if_missing), read_only=read_only)

    def __contains__(self, key):
        return self.db.get(key.encode()) is not None

    def __getitem__(self, key):
        item = self.db.get(key.encode())
        if item is None:
            raise KeyError(key)
        return self.deserialize(item)

    def __setitem__(self, key, value):
        self.db.put(key.encode(), value.encode())

    def __delitem__(self, key):
        self.db.delete(key.encode())

    def __len__(self):
        assert False, "Does not support this operator"

    def get(self, key: str, default=None):
        item = self.db.get(key.encode())
        if item is None:
            return None
        return self.deserialize(item)

    def cache_dict(self) -> 'CacheDictStore[K, V]':
        return CacheDictStore(self)

    def deserialize(self, value):
        return value


class CacheDictStore(Dict[K, V]):
    def __init__(self, store: Dict[K, V]):
        self.store = store
        self.cache = {}

    def __contains__(self, item: str):
        return item in self.cache or item in self.store

    def __getitem__(self, item: str):
        if item not in self.cache:
            self.cache[item] = self.store[item]
        return self.cache[item]

    def __setitem__(self, key: str, value: Union[str, bytes]):
        raise Exception("NotSupportedFunction")

    def __delitem__(self, key):
        raise Exception("NotSupportedFunction")

    def values(self):
        return self.store.values()

    def items(self):
        return self.store.items()

    def keys(self):
        return self.store.keys()

    def __len__(self):
        return len(self.store)
