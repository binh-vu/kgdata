from __future__ import annotations

from dataclasses import dataclass

import orjson


@dataclass
class Record:
    """A base class that provides default serialization and deserialization."""

    def ser(self) -> bytes:
        return orjson.dumps(self, option=orjson.OPT_SERIALIZE_DATACLASS)

    @classmethod
    def deser(cls, o: str):
        return cls(**orjson.loads(o))


@dataclass
class Resource(Record):
    id: str
    props: dict[str, list[str]]
