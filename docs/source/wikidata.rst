Wikidata
========

::

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

You need the following dumps:

1. entity dump
   (`latest-all.json.bz2 <https://dumps.wikimedia.org/wikidatawiki/entities/20200518/wikidata-20200518-all.json.bz2>`__):
   needed to extract entities, classes and properties.
2. ``wikidatawiki-page.sql.gz`` and ``wikidatawiki-redirect.sql.gz``
   (`link <https://dumps.wikimedia.org/wikidatawiki>`__): needed to
   extract redirections of old entities.

Then, execute the following steps:

1. Download the wikidata dumps (e.g.,
   `latest-all.json.bz2 <https://dumps.wikimedia.org/wikidatawiki/entities/20200518/wikidata-20200518-all.json.bz2>`__)
   and put it to ``<wikidata_dir>/step_0`` folder.
2. Extract entities, entity Labels, and entity redirections:

   -  ``kgdata wikidata entities -d <wikidata_dir> -o <database_directory> -c``
   -  ``kgdata wikidata entity_labels -d <wikidata_dir> -o <database_directory> -c``
   -  ``kgdata wikidata entity_redirections -d <wikidata_dir> -o <database_directory> -c``

3. Extract ontology:

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