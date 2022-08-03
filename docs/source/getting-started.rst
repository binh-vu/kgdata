.. include:: glossary.rst

Getting Started
===============

Installation
------------

|kgdata| is distributed on `PyPI <https://pypi.org/project/kgdata/>`_ so you can install with pip as below.

.. code:: bash

    pip install kgdata


.. note::

    You need to have gcc in order for pip to compile `cityhash`. 
    Also, this library uses Apache Spark 3.3.0 (``pyspark`` version is ``3.3.0``).
    If you use different Spark version, try to install from source (see :ref:`Install from Source`).


Try pre-built datasets/databases
--------------------------------

Several datasets/databases are pre-built and can be downloaded `here <https://drive.google.com/drive/folders/1BVQSp2wi4KCu9F8vpbdfZMSwDMnXL6jt?usp=sharing>`_.

For example, download ``wdentity`` and put it to the database folder. Then, you can load and use the database as follow.

.. code:: python

    >>> from kgdata.wikidata.db import get_entity_db

    >>> wdents = get_entity_db(<path to db>)

    >>> print(wdents['Q30'].label)
    United States


Create your own datasets/databases
----------------------------------

For processing and creating your own databases/datasets, see specific pages for each KGs (:ref:`Wikidata` and :ref:`Wikipedia`).