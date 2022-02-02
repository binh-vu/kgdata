KGData is a library to process dumps of knowledge graphs.

## Usage

### Wikidata

```
Usage: python -m kgdata.cli wikidata [OPTIONS]

Options:
  -b, --build TEXT      Build database
  -d, --directory TEXT  Wikidata directory
  -o, --output TEXT     Output directory
  -c, --compact         Whether to compact the db after extraction
  --help                Show this message and exit.
```

1. Download the wikidata dumps (e.g., [`latest-all.json.bz2`](https://dumps.wikimedia.org/wikidatawiki/entities/20200518/wikidata-20200518-all.json.bz2)) and put it to `<wikidata_dir>/step_0` folder.
2. Extract Qnodes: `kgdata wikidata -d <wikidata_dir> -b qnodes -o <database_directory> -c`
3. Extract ontology:
   - `kgdata wikidata -d <wikidata_dir> -b wdclasses -o <database_directory>`
   - `kgdata wikidata -d <wikidata_dir> -b wdprops -o <database_directory>`

For more commands, see `scripts/build.sh`.

## Installation

### From pip

You need to have gcc in order to install `cityhash`

```bash
pip install kgdata
```

### From Source

This library uses Apache Spark 3.0.3 (`pyspark` version is `3.0.3`). If you use different Spark version, make sure that version of `pyspark` package is matched (in `pyproject.toml`).

```bash
poetry install
mkdir dist; zip -r kgdata.zip kgdata; mv kgdata.zip dist/ # package the application to submit to Spark cluster
```

You can also consult the [Dockerfile](./Dockerfile) for guidance to install from scratch.
