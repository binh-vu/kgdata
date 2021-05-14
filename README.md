KGData is a library to process dumps of knowledge graphs.

## Installation

### From Source

This library uses Apache Spark 3.0.1 (`pyspark` version is `3.0.1`). If you use different Spark version, make sure that version of `pyspark` package is matched (in `pyproject.toml`).

```bash
poetry install
poetry build # build a wheel version to submit to Spark cluster
```

