from importlib import import_module
from typing_extensions import Required
import click
from kgdata.wikipedia.config import WPDataDirConfig
import kgdata.wikipedia.datasets
from loguru import logger


@click.command("Generate a specific dataset")
@click.option("-s", "--source", required=True, help="Wikipedia directory")
@click.option("-d", "--dataset", required=True, help="Dataset name")
def main(source: str, dataset: str):
    logger.info("Wikidata directory: {}", source)

    WPDataDirConfig.init(source)

    module = import_module(f"kgdata.wikipedia.datasets.{dataset}")
    getattr(module, dataset)()


if __name__ == "__main__":
    main()
