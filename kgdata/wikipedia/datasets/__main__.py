"""
"""

from importlib import import_module

import click

from kgdata.config import init_dbdir_from_env
from kgdata.dataset import make_dataset_cli

if __name__ == "__main__":
    make_dataset_cli("wikipedia")()
