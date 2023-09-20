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


    Client for BUILDA, the European building database.


This is an HTTP-client that provides methods for accessing the API endpoints (see e.g. http://134.94.116.65:8000/api/v1_20220831/swagger/ for the first version) of the European building database (BUILDA).

Installation
====
You can install a specific release directly from remote repository via:

.. code-block:: console

    pip install git+https://jugit.fz-juelich.de/iek-3/groups/urbanmodels/personal/dabrock/building-database-builda/builda-client.git@v1.0 

If you execute this command without specifying the tag/release, you will install the main branch. This branch is under development and will change based on the newest (not yet versioned) API and database modifications. It is not stable. 

You can also install by downloading the repo to a local folder, checking out the tag/branch you need, cd-ing into it and installing the client into an environment of your choice (e.g. conda) via:

.. code-block:: console

    pip install .

For development, install in editable mode with:

.. code-block:: console

    pip install -e .

And if you want to execute tests:

.. code-block:: console

    pip install -e .[testing]

Or for development and testing:

.. code-block:: console 

    pip install -e ".[test,development]"

Usage 
====

Import the client via:

.. code-block:: python

    from builda_client.client import ApiClient

And instantiate client like this:

.. code-block:: python

    client: ApiClient = ApiClient()

This is sufficient for the standard case. 

If you need to use a proxy because you are executing your code on the cluster compute nodes, you have to tell the client on instantiation:

.. code-block:: python

    client: ApiClient = ApiClient(use_proxy=True)

Now you can use the methods provided by the client, e.g.:

.. code-block:: python

    buildings: list[Building] = client.get_buildings()

Some methods require authentication. You can recognize those by the comment [REQUIRES AUTHENTICATION] at the beginning of the method's docstring.
To use these methods the client has to be instantiated with a valid username and password.


How to create new version
====

1. Set the base_url in config.yml to the new version of the API.
2. Test if the client still works (test_client_read.py)
3. Merge changes into main branch
4. Tag with version (e.g. v1.0)
5. Change base_url back to /api/v0 for further development and merge changes into main

If you need to do changes to a version later, check out a new branch at the tag.

Create documentation via `tox -e docs`

.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.
