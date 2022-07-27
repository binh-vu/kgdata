.. include:: glossary.rst

Wikipedia
=========

.. admonition:: Prerequisite

   Please see :ref:`Wikipedia dumps` for the list of dumps that are required to generate all datasets.

.. admonition:: Data organization

   |kgdata| organizes Wikipedia's data in a folder ``<wikipedia_dir>``, e.g., ``/data/wikipedia/20200518``. The dumps are stored in a subfolder called dumps ``<wikipedia_dir>/dumps`` (e.g., ``/data/wikipedia/20200518/dumps``). Other datasets after processed are stored in sibling folders. The list of folders can be found in :py:mod:`kgdata.wikipedia.config`.


Wikipedia datasets
-----------------

List of available datasets can be found in :py:mod:`kgdata.wikipedia.datasets`.

.. automodule:: kgdata.wikipedia.datasets.__main__


Wikipedia dumps
---------------

We need the following dumps:

1. `Static HTML Dumps <https://dumps.wikimedia.org/other/enterprise_html/>`__: they only dumps some namespaces. The namespace that you likely to use is 0 (main articles). For example, ``enwiki-NS0-20220420-ENTERPRISE-HTML.json.tar.gz``.

Below are some useful scripts to download the dumps. First, set the correct parameters:

.. code:: bash

   export VERSION=20200420
   export DIR=<wikipedia_dir>

.. code:: bash

   mkdir -p $DIR/dumps
   cd $DIR/dumps
   wget https://dumps.wikimedia.org/other/enterprise_html/runs/$VERSION/enwiki-NS0-$VERSION-ENTERPRISE-HTML.json.tar.gz