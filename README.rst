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

.. image:: https://www.fz-juelich.de/static/media/Logo.2ceb35fc.svg
    :alt: Forschungszentrum Juelich Logo
    :target: https://www.fz-juelich.de/en/iek/iek-3
    :width: 230px

|

====================
ETHOS.BUILDA-Client
====================


    Python client for ETHOS.BUILDA, the German building database.


This is a Python HTTP-client that provides methods for accessing the API endpoints of ETHOS.BUILDA.
The endpoints are documented using Swagger and can be accessed here: http://134.94.130.118/api/v5_20230915/swagger/ (version 5).

ETHOS.BUILDA is a database containing building-level data for the German building stock. 
It is based on various data sources that are combined and enriched with machine learning approaches to generate one consistent and complete building dataset.
The database is released under the `Open Data Commons Open Database License (ODbL) <https://opendatacommons.org/licenses/odbl/>`_.
The sources of the data points and information on the type of processing that was done to assign the information from the raw data to the building in ETHOS.BUILDA are included in the query results.


Installation
============
You can install a specific release directly from remote repository via:

.. code-block:: console

    pip install git+https://github.com/FZJ-IEK3-VSA/ethos-builda-client.git@v5.0 

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
=====

Import and instantiate the client via:

.. code-block:: python

    from builda_client.client import BuildaClient
    client: BuildaClient = BuildaClient()

This is sufficient for the standard case. 

If you need to use a proxy because you are executing your code on the cluster compute nodes, you have to provide the proxy host and port to the client on instantiation:

.. code-block:: python

    buildings: list[BuildingResponseDto] = client.get_buildings()

Some methods require authentication. You can recognize those by the comment [REQUIRES AUTHENTICATION] at the beginning of the method's docstring.
To use these methods the client has to be instantiated with a valid username and password.

If you are a database contributor/internal user, you need to instantiate the development client.
Using this client, you have access to additional methods for querying and writing data.
However, this requires a username and password as the respective API endpoints are not openly available (yet).

.. code-block:: python

    from builda_client.dev_client import BuildaDevClient
    client: BuildaDevClient = BuildaDevClient(username='j.doe', password='secret_password')


How to create new version
==========================

1. Set the base_url in config.yml to the new version of the API.
2. Test if the client still works (test_client_read.py)
3. Merge changes into main branch
4. Tag with version (e.g. v1.0)
5. Change base_url back to /api/v0 for further development and merge changes into main

If you need to do changes to a version later, check out a new branch at the tag.

Create documentation in HTML and LaTeX format via `tox -e docs_html,docs_latex`


Acknowledgements
================
This work was supported by the Helmholtz Association under the program "Energy System Design".

.. image:: https://www.helmholtz.de/fileadmin/user_upload/05_aktuelles/Marke_Design/logos/HG_LOGO_S_ENG_RGB.jpg
    :target: https://www.helmholtz.de/en/
    :alt: Helmholtz Logo
    :width: 200px

.. _pyscaffold-notes:
Note
====

This project has been set up using PyScaffold 4.2.3. For details and usage
information on PyScaffold see https://pyscaffold.org/.
