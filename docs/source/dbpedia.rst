.. include:: glossary.rst

DBpedia
========

.. admonition:: Prerequisite

   Please see :ref:`DBpedia dumps` for the list of dumps that are required to generate all datasets.

.. admonition:: Data organization

   |kgdata| organizes DBpedia's data in a folder ``<dbpedia_dir>``, e.g., ``/data/dbpedia/20200518``. The dumps are stored in a subfolder called dumps ``<dbpedia_dir>/dumps`` (e.g., ``/data/dbpedia/20200518/dumps``). Other datasets after processed are stored in sibling folders. The list of folders can be found in :py:mod:`kgdata.dbpedia.config`.

DBpedia datasets
-----------------

List of available datasets can be found in :py:mod:`kgdata.dbpedia.datasets`.

.. automodule:: kgdata.dbpedia.datasets.__main__

DBpedia databases
------------------

List of available databases can be found by running ``python -m kgdata.dbpedia``.

.. code:: bash

   $ python -m kgdata.dbpedia --help

   Usage: python -m kgdata.dbpedia [OPTIONS] COMMAND [ARGS]...

   Options:
     --help  Show this message and exit.

   Commands:
     classes              DBpedia classes
     properties           DBpedia properties

DBpedia dumps
--------------

The dumps are available at `databus.dbpedia.org <https://databus.dbpedia.org/>`__. DBpedia snapshot can be found in here: `dbpedia snapshot 2022-12 https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12`__

We need the following dumps:

1. ontology dump (e.g., `ontology_type=parsed.ttl https://databus.dbpedia.org/ontologies/dbpedia.org/ontology/2023.05.12-020000/ontology_type=parsed.ttl`__): needed to extract classes and properties. Note: somehow the Ontology-DEV version contains more information similar to the DBpedia endpoint than the release version.
2. `mapping extractor dumps https://databus.dbpedia.org/dbpedia/mappings`__
3. `generic extractor dumps https://databus.dbpedia.org/dbpedia/generic`__
4. `redirect dumps https://databus.dbpedia.org/dbpedia/generic/redirects`__