from __future__ import annotations

from typing import Any, Callable, Literal


class PropQuery:
    def __init__(self, prop: list[str | int], value: str, op: Literal["=", "in"]):
        self.prop = prop
        self.value = value
        self.op = op

    def __call__(self, obj: Any):
        if self.op == "=":
            return str(getprop(obj, self.prop)) == self.value
        elif self.op == "in":
            return self.value in getprop(obj, self.prop)
        else:
            raise NotImplementedError()

    @staticmethod
    def from_string(s: str) -> PropQuery:
        if "=" in s:
            prop, value = s.split("=", 1)
            op = "="
        else:
            assert " in " in s
            value, prop = s.split(" in ", 1)
            op = "in"
        prop = [int(name) if name.isdigit() else name for name in prop.split(".")]
        return PropQuery(prop, value, op)


def every(queries: list[PropQuery]) -> Callable[[Any], bool]:
    def f(obj: Any) -> bool:
        return all(q(obj) for q in queries)

    return f


def getprop(obj: Any, prop: list[str | int]):
    for name in prop:
        if isinstance(name, str) and hasattr(obj, name):
            obj = getattr(obj, name)
        else:
            obj = obj[name]
    return obj
