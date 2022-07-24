Quickstart
==========

From PyPI
---------

kgdata is distributed on `PyPI <https://pypi.org/project/kgdata/>`_. You need to have gcc in order for pip to compile `cityhash`

.. code:: bash

    pip install kgdata


From Source
-----------

This library uses Apache Spark 3.0.3 (``pyspark`` version is ``3.0.3``). If you use different Spark version, make sure that version of ``pyspark`` package is matched (in ``pyproject.toml``).

.. code:: bash
    
    poetry install
    mkdir dist; zip -r kgdata.zip kgdata; mv kgdata.zip dist/ # package the application to submit to Spark cluster


You can also consult the :source:`Dockerfile` for guidance to install from scratch.
