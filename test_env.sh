export PYTHONPATH=./logiclibrary:./mongodbstorage:../lda-clientlib/python
export APP_NAME=testlda
export MONGODB_DB_HOST=localhost
export MONGODB_DB_PORT=27017
python setup.py install
#
# python
# >>> import os
# >>> os.environ['APP_NAME'] = 'mytest'
# >>> os.environ['MONGODB_DB_HOST'] = 'localhost'
# >>> os.environ['MONGODB_DB_PORT'] = '27017'
# >>> import lda
# >>> foo_container_environ = {'REQUEST_URI': '/tst/foo', 'HTTP_HOST': 'localhost', 'PATH_INFO': '/tst/foo', 'QUERY_STRING': ''}
# >>> new_foo_resource = {'rdfs_label': 'my foo', 'rdf_type': 'http://example.org#Foo'}
# >>> body, status, headers = lda.create_document(foo_container_environ, new_foo_resource, 'in_foo_container')
# INFO:operation_primitives:created document http://localhost/tst/foo/3.5
# >>> body
# {   '_subject': 'http://localhost/tst/foo/3.5',
#     'ac_resource-group': 'http://localhost/',
#     'ce_lastModified': '2015-04-16T17:10:58.594000+00:00',
#     'ce_lastModifiedBy': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#     'ce_owner': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#     'ce_revision': '0',
#     'dc_created': '2015-04-16T17:10:58.594000+00:00',
#     'dc_creator': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#     'in_foo_container': 'http://localhost/tst/foo',
#     'rdf_type': 'http://example.org#Foo',
#     'rdfs_label': 'my foo'}
# >>> status
# 201
# >>> headers
# [('Location', 'http://localhost/tst/foo/1.1')]
# >>> body, status, headers = lda.create_document(foo_container_environ, new_foo_resource, 'in_foo_container')
# INFO:operation_primitives:created document http://localhost/tst/foo/3.6
# >>> body, status, headers = lda.get_virtual_container(foo_container_environ, 'in_foo_container')
# >>> body
# {   '_subject': 'http://localhost/tst/foo',
#     'ac_resource-group': 'http://localhost/',
#     'ldp_contains': [   {   '_subject': 'http://localhost/tst/foo/3.5',
#                             'ac_resource-group': 'http://localhost/',
#                             'ce_lastModified': '2015-04-16T17:05:10.772000+00:00',
#                             'ce_lastModifiedBy': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'ce_owner': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'ce_revision': '0',
#                             'dc_created': '2015-04-16T17:05:10.772000+00:00',
#                             'dc_creator': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'in_foo_container': 'http://localhost/tst/foo',
#                             'rdf_type': 'http://example.org#Foo',
#                             'rdfs_label': 'my foo'},
#                         {   '_subject': 'http://localhost/tst/foo/3.6',
#                             'ac_resource-group': 'http://localhost/',
#                             'ce_lastModified': '2015-04-16T17:05:13.003000+00:00',
#                             'ce_lastModifiedBy': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'ce_owner': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'ce_revision': '0',
#                             'dc_created': '2015-04-16T17:05:13.003000+00:00',
#                             'dc_creator': 'http://localhost/unknown_user/03367cce-5cfd-43a7-bb5e-d6ccae7439dd',
#                             'in_foo_container': 'http://localhost/tst/foo',
#                             'rdf_type': 'http://example.org#Foo',
#                             'rdfs_label': 'my foo'}],
#     'ldp_isMemberOfRelation': 'in_foo_container',
#     'ldp_membershipResource': 'http://localhost/tst/foo',
#     'rdf_type': 'http://www.w3.org/ns/ldp#DirectContainer'}
# >>>
