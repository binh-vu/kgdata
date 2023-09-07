from __future__ import annotations


def split_tab_2(x: str) -> tuple[str, str]:
    k, v = x.split("\t")
    return (k, v)
