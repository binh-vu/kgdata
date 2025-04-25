# kgdata ![PyPI](https://img.shields.io/pypi/v/kgdata) ![Documentation](https://readthedocs.org/projects/kgdata/badge/?version=latest&style=flat)

KGData is a library to process dumps of Wikipedia, Wikidata. What it can do:

- Clean up the dumps to ensure the data is consistent (resolve redirect, remove dangling references)
- Create embedded key-value databases to access entities from the dumps.
- Extract Wikidata ontology.
- Extract Wikipedia tables and convert the hyperlinks to Wikidata entities.
- Create Pyserini indices to search Wikidata’s entities.
- and more

For a full documentation, please see [the website](https://kgdata.readthedocs.io/).

## Installation

From PyPI (using pre-built binaries):

```bash
pip install kgdata[spark]   # omit spark to manually specify its version if your cluster has different version
```
