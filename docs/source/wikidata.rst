.. include:: glossary.rst

Wikidata
========

.. admonition:: Prerequisite

   Please see :ref:`Wikidata dumps` for the list of dumps that are required to generate all datasets.

.. admonition:: Data organization

   |kgdata| organizes Wikidata's data in a folder ``<wikidata_dir>``, e.g., ``/data/wikidata/20200518``. The dumps are stored in a subfolder called dumps ``<wikidata_dir>/dumps`` (e.g., ``/data/wikidata/20200518/dumps``). Other datasets after processed are stored in sibling folders. The list of folders can be found in :py:mod:`kgdata.wikidata.config`.

Wikidata datasets
-----------------

List of available datasets can be found in :py:mod:`kgdata.wikidata.datasets`.

.. automodule:: kgdata.wikidata.datasets.__main__

Wikidata databases
------------------

List of available databases can be found by running ``python -m kgdata.wikidata``.

.. code:: bash

   $ python -m kgdata.wikidata --help

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

We provide functions to read the databases built from the previous step
and return a dictionary-like objects in the module:
:py:mod:`kgdata.wikidata.db`. You can find main models of Wikidata in here:
:py:mod:`kgdata.wikidata.models.wdentity`, :py:mod:`kgdata.wikidata.models.wdclass`, :py:mod:`kgdata.wikidata.models.wdproperty`.

1. Extract entities, entity Labels, and entity redirections:

   -  ``kgdata wikidata entities -d <wikidata_dir> -o <database_directory> -c``
   -  ``kgdata wikidata entity_labels -d <wikidata_dir> -o <database_directory> -c``
   -  ``kgdata wikidata entity_redirections -d <wikidata_dir> -o <database_directory> -c``

2. Extract ontology:

   -  ``kgdata wikidata classes -d <wikidata_dir> -o <database_directory> -c``
   -  ``kgdata wikidata properties -d <wikidata_dir> -o <database_directory> -c``

For more commands, see ``scripts/build.sh``. If compaction step (compact
rocksdb) takes lots of time, you can run without ``-c`` flag. If you run
directly from source, replacing the ``kgdata`` command with
``python -m kgdata``.

We provide functions to read the databases built from the previous step
and return a dictionary-like objects in the module:
:py:mod:`kgdata.wikidata.db`. You can find main models of Wikidata in here:
:py:mod:`kgdata.wikidata.models.wdentity`, :py:mod:`kgdata.wikidata.models.wdclass`, :py:mod:`kgdata.wikidata.models.wdproperty`.

Wikidata dumps
--------------

The dumps are available at `dumps.wikimedia.org <https://dumps.wikimedia.org/wikidatawiki/>`__.

We need the following dumps:

1. entity dump
   (e.g., `latest-all.json.bz2 <https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2>`__):
   needed to extract entities, classes and properties.
2. `wikidatawiki-page.sql.gz <https://dumps.wikimedia.org/wikidatawiki/latest/wikidatawiki-latest-page.sql.gz>`__ and `wikidatawiki-redirect.sql.gz <https://dumps.wikimedia.org/wikidatawiki/latest/wikidatawiki-latest-redirect.sql.gz>`__: needed to
   resolve redirections of old entities.

Below are some useful scripts to download the dumps. First, set the correct parameters:

.. code:: bash

   export VERSION=20200518
   export DIR=<wikidata_dir>

.. code:: bash

   mkdir -p $DIR/dumps
   cd $DIR/dumps
   wget https://dumps.wikimedia.org/wikidatawiki/entities/$VERSION/wikidata-$VERSION-all.json.bz2
   wget https://dumps.wikimedia.org/wikidatawiki/$VERSION/wikidatawiki-$VERSION-page.sql.gz
   wget https://dumps.wikimedia.org/wikidatawiki/$VERSION/wikidatawiki-$VERSION-redirect.sql.gz