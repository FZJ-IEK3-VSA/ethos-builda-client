.. These are examples of badges you might want to add to your README:
   please update the URLs accordingly

    .. image:: https://api.cirrus-ci.com/github/<USER>/builda-client.svg?branch=main
        :alt: Built Status
        :target: https://cirrus-ci.com/github/<USER>/builda-client
    .. image:: https://readthedocs.org/projects/builda-client/badge/?version=latest
        :alt: ReadTheDocs
        :target: https://builda-client.readthedocs.io/en/stable/
    .. image:: https://img.shields.io/coveralls/github/<USER>/builda-client/main.svg
        :alt: Coveralls
        :target: https://coveralls.io/r/<USER>/builda-client
    .. image:: https://img.shields.io/pypi/v/builda-client.svg
        :alt: PyPI-Server
        :target: https://pypi.org/project/builda-client/
    .. image:: https://img.shields.io/conda/vn/conda-forge/builda-client.svg
        :alt: Conda-Forge
        :target: https://anaconda.org/conda-forge/builda-client
    .. image:: https://pepy.tech/badge/builda-client/month
        :alt: Monthly Downloads
        :target: https://pepy.tech/project/builda-client
    .. image:: https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Twitter
        :alt: Twitter
        :target: https://twitter.com/builda-client

.. image:: https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold
    :alt: Project generated with PyScaffold
    :target: https://pyscaffold.org/

|

=============
builda-client
=============


    Client for BUILDA, the European building database


This is an HTTP-Client that provides methods for accessing the API endpoints that provide access to the European building database (BUILDA).


For using the client, create an environment of your choice (e.g. conda) and install the client into it via:
.. code-block:: bash
    pip install .

Or install directly from remote repository via:
.. code-block:: bash
    pip install git+https://jugit.fz-juelich.de/iek-3/groups/urbanmodels/personal/dabrock/builda-client.git 

For development, install in editable mode with:
.. code-block:: bash
    pip install -e .

And if you want to execute tests:
.. code-block:: bash
    pip install -e .[testing]


.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.
