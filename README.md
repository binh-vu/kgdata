KGData is a library to process dumps of knowledge graphs.

## Usage

### Wikidata

1. Download the wikidata dumps
2. Run `wikidata.s00_prepr_data` to preprocess the dump
2. Run other commands in the wikidata package to: extract ontology, qnodes, and schema.

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