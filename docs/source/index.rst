.. include:: glossary.rst
.. kgdata documentation master file, created by
   sphinx-quickstart on Fri Jul 22 19:47:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

|kgdata| documentation
==================================

|kgdata| is a Python library to process dumps of Wikipedia, Wikidata. What it can do:

- Clean up the dumps to ensure the data is consistent (resolve redirect, remove dangling references)
- Create embedded key-value databases to access entities from the dumps.
- Extract Wikidata ontology.
- Extract Wikipedia tables and convert the hyperlinks to Wikidata entities.
- Create Pyserini indices to search Wikidata's entities.

.. toctree::
   :maxdepth: 1
   :caption: Contents

   getting-started
   wikidata
   wikipedia
   api

.. toctree::
   :maxdepth: 1
   :hidden: 
   :caption: Development

   development
   changelog



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
