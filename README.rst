LD4Apps (Linked Data for Applications)
======================================

LD4Apps is an Apache2 licensed Python library for Linked Data for Applications (LDA).
LDA helps you implement applications that will be part of a bigger coherent system, for 
example a web-site made up of multiple applications that talk to each other. LDA
applications are system components that communicate entirely through HTTP/REST
interfaces.

Installation
------------

To install LD4Apps:

.. code-block:: bash

    pip install ld4apps

The LDA framework requires a running back-end database for storing application data. 
The default storage implementation uses MongoDB, so the fastest way to get started is
to simply go to the `MongoDB download page <https://www.mongodb.org/downloads>`_ and download 
the appropriate version for your OS.

Once MongoDB has downloaded, execute the following commands:

.. code-block:: bash

    cd <mongodb-installation-directory>/bin
    mongod

At this point, MonoDB should be running and listening on its default host and port (localhost:27017).

To configure LDA with the MongoDB server, 3 environment variables also need to be set:
    
1. MONGODB_DB_HOST - hostname of the MONGODB server to use
2. MONGODB_DB_PORT - the MONGDOB server port
3. APP_NAME - the name of you application, which is used as the DB name where the resources will be stored

Example
-------

You can create a Linked-Data resource using the *ld4apps.lda* module like this:

.. code-block:: python

    >>> import os
    >>> os.environ['MONGODB_DB_HOST'] = 'localhost'
    >>> os.environ['MONGODB_DB_PORT'] = '27017'
    >>> os.environ['APP_NAME'] = 'teststore'
    >>> from ld4apps import lda
    >>> foo_container_environ = {'HTTP_HOST': 'localhost', 'PATH_INFO': '/tst/foo', 'QUERY_STRING': ''}
    >>> new_foo_resource = {'rdfs_label': 'my foo', 'rdf_type': 'http://example.org#Foo'}
    >>> body, status, headers = lda.create_document(foo_container_environ, new_foo_resource, 'in_foo_container')
    INFO:ld4apps.mongodbstorage.operation_primitives:created document http://localhost/tst/foo/4.1
    >>> status
    201
    >>> import json
    >>> print json.dumps(body, indent=4)
    {
        "ce_owner": "http://localhost/unknown_user/da36bb01-0d1c-438a-9d00-9940085aae20",
        "rdfs_label": "my foo",
        "in_foo_container": "http://localhost/tst/foo",
        "ce_lastModified": "2015-04-28T18:30:40.993176+00:00",
        "dc_created": "2015-04-28T18:30:40.993176+00:00",
        "ac_resource-group": "http://localhost/",
        "_subject": "http://localhost/tst/foo/3.2",
        "ce_revision": "0",
        "ce_lastModifiedBy": "http://localhost/unknown_user/da36bb01-0d1c-438a-9d00-9940085aae20",
        "dc_creator": "http://localhost/unknown_user/da36bb01-0d1c-438a-9d00-9940085aae20",
        "rdf_type": "http://example.org#Foo"
    }

An example using ld4apps along with `Flask <http://flask.pocoo.org/>`_ to implement a simple "todo list" web-server 
can be found here: https://github.com/ld4apps/lda-examples/tree/ld4apps/todo-flask.

Documentation
-------------

Documentation is available at http://ld4apps.github.io/.
