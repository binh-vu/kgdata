import click
from kgdata.wikidata.__main__ import wikidata
from kgdata.wikipedia.cli import wikipedia


@click.group()
def cli():
    pass


cli.add_command(wikidata)
cli.add_command(wikipedia)


if __name__ == "__main__":
    cli()
