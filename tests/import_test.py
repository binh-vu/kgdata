import pkgutil
import kgdata
from pathlib import Path
from importlib import import_module


def test_import(pkg=kgdata, ignore_deprecated: bool = True):
    stack = [(pkg.__name__, Path(pkg.__file__).parent.absolute())]

    while len(stack) > 0:
        pkgname, pkgpath = stack.pop()
        for m in pkgutil.iter_modules([str(pkgpath)]):
            mname = f"{pkgname}.{m.name}"
            if ignore_deprecated and mname.find("deprecated") != -1:
                continue
            if m.ispkg:
                stack.append((mname, pkgpath / m.name))
            import_module(mname)
