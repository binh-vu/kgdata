from __future__ import annotations

import importlib
from io import BufferedReader, BytesIO, TextIOWrapper
from typing import Type

import zstandard as zstd


def split_tab_2(x: str) -> tuple[str, str]:
    k, v = x.split("\t")
    return (k, v)


TYPE_ALIASES = {"typing.List": "list", "typing.Dict": "dict", "typing.Set": "set"}


def get_import_path(type: Type) -> str:
    if type.__module__ == "builtins":
        return type.__qualname__

    if hasattr(type, "__qualname__"):
        return type.__module__ + "." + type.__qualname__

    # typically a class from the typing module
    if hasattr(type, "_name") and type._name is not None:
        path = type.__module__ + "." + type._name
        if path in TYPE_ALIASES:
            path = TYPE_ALIASES[path]
    elif hasattr(type, "__origin__") and hasattr(type.__origin__, "_name"):
        # found one case which is typing.Union
        path = type.__module__ + "." + type.__origin__._name
    else:
        raise NotImplementedError(type)

    return path


def import_attr(attr_ident: str):
    lst = attr_ident.rsplit(".", 1)
    module, cls = lst
    module = importlib.import_module(module)
    return getattr(module, cls)


def deser_zstd_records(dat: bytes):
    cctx = zstd.ZstdDecompressor()
    datobj = BytesIO(dat)
    # readlines will result in an extra \n at the end
    # we do not want this because it's different from spark implementation
    return cctx.stream_reader(datobj).readall().splitlines()
