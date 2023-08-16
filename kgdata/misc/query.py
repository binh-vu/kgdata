from __future__ import annotations

from typing import Any, Callable


class PropEqualQuery:
    def __init__(self, prop: list[str], value: str):
        self.prop = prop
        self.value = value

    def __call__(self, obj: Any):
        return str(getprop(obj, self.prop)) == self.value

    @staticmethod
    def from_string(s: str) -> PropEqualQuery:
        prop, value = s.split("=", 1)
        prop = prop.split(".")
        return PropEqualQuery(prop, value)


def every(queries: list[PropEqualQuery]) -> Callable[[Any], bool]:
    def f(obj: Any) -> bool:
        return all(q(obj) for q in queries)

    return f


def getprop(obj: Any, prop: list[str]):
    for name in prop:
        if hasattr(obj, name):
            obj = getattr(obj, name)
        elif name in obj:
            obj = obj[name]
    return obj
