from importlib import import_module

from kgdata.dataset import Dataset


def import_dataset(dataset: str) -> Dataset:
    module = import_module(f"kgdata.wikipedia.datasets.{dataset}")
    return getattr(module, dataset)()
