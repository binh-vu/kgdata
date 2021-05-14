KGData is a library to process dumps of knowledge graphs.

## Usage

### Wikidata

1. Download the wikidata dumps
2. Run `wikidata.s00_prepr_data` to preprocess the dump
2. Run other commands in the wikidata package to: extract ontology, qnodes, and schema.

## Installation

### From Source

This library uses Apache Spark 3.0.1 (`pyspark` version is `3.0.1`). If you use different Spark version, make sure that version of `pyspark` package is matched (in `pyproject.toml`).

```bash
poetry install
poetry build # build a wheel version to submit to Spark cluster
```

