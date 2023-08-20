from importlib import import_module
from typing import Optional

from kgdata.dataset import Dataset


def import_dataset(dataset: str, kwargs: Optional[dict] = None) -> Dataset:
    module = import_module(f"kgdata.wikidata.datasets.{dataset}")
    kwargs = kwargs or {}
    return getattr(module, dataset)(**kwargs)
    return getattr(module, dataset)(**kwargs)
