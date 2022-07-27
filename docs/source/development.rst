Development
===========

Install from Source
-------------------

.. code:: bash

    pip install .
    mkdir dist; zip -r kgdata.zip kgdata; mv kgdata.zip dist/ # package the application to submit to Spark cluster

You can also consult the :source:`Dockerfile` for guidance to install from scratch.

Setup Documentation
-------------------

1. Installing dependencies & copying required files

.. code:: bash

    pip install .
    pip install -r docs/requirements.txt
    cp CHANGELOG.md docs/source/changelog.md

2. Run ``sphinx-autobuild``

.. code:: bash

    sphinx-autobuild docs/source docs/build/html
