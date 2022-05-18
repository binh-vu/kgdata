KGData is a library to process dumps of Wikipedia, Wikidata and DBPedia.

![PyPI](https://img.shields.io/pypi/v/kgdata)

## Contents

<!--ts-->

- [Usage](#usage)
  - [Wikidata](#wikidata)
  - [Wikipedia](#wikipedia)
- [Installation](#installation)
<!--te-->

## Usage

### Wikidata

```
Usage: python -m kgdata.wikidata [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  classes              Wikidata classes
  entities             Wikidata entities
  entity_labels        Wikidata entity labels
  entity_redirections  Wikidata entity redirections
  properties           Wikidata properties
  wp2wd                Mapping from Wikipedia articles to Wikidata entities
```

You need the following dumps:

1. entity dump ([`latest-all.json.bz2`](https://dumps.wikimedia.org/wikidatawiki/entities/20200518/wikidata-20200518-all.json.bz2)): needed to extract entities, classes and properties.
2. `wikidatawiki-page.sql.gz` and `wikidatawiki-redirect.sql.gz` ([link](https://dumps.wikimedia.org/wikidatawiki)): needed to extract redirections of old entities.

Then, execute the following steps:

1.  Download the wikidata dumps (e.g., [`latest-all.json.bz2`](https://dumps.wikimedia.org/wikidatawiki/entities/20200518/wikidata-20200518-all.json.bz2)) and put it to `<wikidata_dir>/step_0` folder.
1.  Extract entities, entity Labels, and entity redirections:
    - `kgdata wikidata entities -d <wikidata_dir> -o <database_directory> -c`
    - `kgdata wikidata entity_labels -d <wikidata_dir> -o <database_directory> -c`
    - `kgdata wikidata entity_redirections -d <wikidata_dir> -o <database_directory> -c`
1.  Extract ontology:
    - `kgdata wikidata classes -d <wikidata_dir> -o <database_directory> -c`
    - `kgdata wikidata properties -d <wikidata_dir> -o <database_directory> -c`

For more commands, see `scripts/build.sh`.
If compaction step (compact rocksdb) takes lots of time, you can run without `-c` flag.
If you run directly from source, replacing the `kgdata` command with `python -m kgdata`.

We provide functions to read the databases built from the previous step and return a dictionary-like objects in the module: [`kgdata.wikidata.db`](/kgdata/wikidata/db.py). In the same folder, you can find models of Wikidata [entities](/kgdata/wikidata/models/qnode.py), [classes](/kgdata/wikidata/models/wdclass.py), and [properties](/kgdata/wikidata/models/wdproperty.py).

### Wikipedia

Here is a list of dumps that you need to download depending on the database/files you want to build:

1. [Static HTML Dumps](https://dumps.wikimedia.org/other/enterprise_html/): they only dumps some namespaces. The namespace that you likely to use is 0 (main articles). For example, enwiki-NS0-20220420-ENTERPRISE-HTML.json.tar.gz.

Then, execute the following steps:

1. Extract HTML Dumps:
   - `kgdata wikipedia -d <wikipedia_dir> enterprise_html_dumps`

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
