from __future__ import annotations

from pathlib import Path

import click
import serde.json

from kgdata.spark.extended_rdd import DatasetSignature

"""This script aggregates signatures of all datasets in the data folder."""


@click.command()
@click.argument("input_dir", type=click.Path(exists=True))
def make_signatures(input_dir: Path):
    signatures = {}
    for sigfile in sorted(input_dir.glob("**/*/_SIGNATURE")):
        sig = DatasetSignature.from_dict(serde.json.deser(sigfile))
        signatures[sig.name] = {
            "created_at": sig.created_at,
            "checksum": sig.checksum,
            "dependencies": sorted(sig.dependencies.keys()),
        }
    return signatures


if __name__ == "__main__":
    make_signatures()
