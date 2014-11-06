import operation_primitives
import urlparse, urllib
import json, rdf_json
from rdf_json import URI
from trsbuilder import TrackedResourceSetBuilder
import utils
import os
import requests
from requests.exceptions import ConnectionError
from base_constants import RDF, LDP, CE, OWL, TRS, AC, AC_R, AC_C, AC_ALL, ADMIN_USER, NAMESPACE_MAPPINGS
from base_constants import URL_POLICY as url_policy
import logging

logger=logging.getLogger(__name__)

HISTORY = CE+'history'
CREATION_EVENT = TRS+'Creation'
MODIFICATION_EVENT = TRS+'Modification'
DELETION_EVENT = TRS+'Deletion'

CHECK_ACCESS_RIGHTS = os.environ.get('CHECK_ACCESS_RIGHTS') != 'False'
UNCHANGED=object() # special value for recurse() args

class Domain_Logic(object):
    def __init__(self, environ, change_tracking=False):
        self.environ = environ
        self.claims = utils.get_or_create_claims(environ)
        self.user = self.claims['user']
        self.url_components = url_policy.get_url_components(environ)
        self.tenant, self.namespace, self.document_id, self.extra_path_segments, self.path, self.path_parts, self.request_hostname, self.query_string = self.url_components
        self.change_tracking = change_tracking # TODO: should we provide a way to turn change_tracking on/off dynamically
        if change_tracking:
            self.trs_builders = {}

    def recurse(self, function, namespace=UNCHANGED, document_id=UNCHANGED, extra_path_segments=UNCHANGED, query_string=UNCHANGED, url=None):
        """
        Perform an operation with the same host-name and tenant, but new document_id, extra_segements and query_string.

        One implementation option would be to make a new instance of Domain_Logic and give it a new environ dict copy.
        This implementation is slightly cheaper/messier.
        """
        if url != None:
            if namespace != UNCHANGED or document_id != UNCHANGED or extra_path_segments != UNCHANGED or query_string != UNCHANGED:
                raise ValueError('may nor set URL and also set namespace, document_id, extra_path_segments or query_string')
            namespace, document_id, extra_path_segments, parse_result = url_policy.parse(url)
            query_string = parse_result.query
        original_namespace = self.namespace
        original_document_id = self.document_id
        original_path = self.path
        original_extra_path_segments = self.extra_path_segments
        original_query_string = self.query_string
        original_path_parts = self.path_parts
        try:
            if namespace != UNCHANGED:
                self.namespace = namespace
            if document_id != UNCHANGED:
                self.document_id = document_id
            if extra_path_segments != UNCHANGED:
                self.extra_path_segments = extra_path_segments
            self.path_parts = ['', self.namespace, self.document_id] if self.namespace and self.document_id else ['', self.namespace] if self.namespace else ['']
            if self.extra_path_segments:
                self.path_parts = self.path_parts + self.extra_path_segments
            self.path = '/'.join(self.path_parts)
            if query_string != UNCHANGED:
                self.query_string = query_string
            status, headers, document = function()
        finally:
            self.namespace = original_namespace
            self.document_id = original_document_id
            self.path = original_path
            self.extra_path_segments = original_extra_path_segments
            self.query_string = original_query_string
            self.path_parts = original_path_parts
        return status, headers, document

    def recursive_get_document(self, namespace=UNCHANGED, document_id=UNCHANGED, extra_path_segments=UNCHANGED, query_string=UNCHANGED, url=None):
        return self.recurse(self.get_document, namespace, document_id, extra_path_segments, query_string, url)

    def create_document(self, document, document_id=None):
        """
        This method is called when a POST is made that means 'create'.

        The 'document' argument is a Python dictionary that was created from a rdf/json (ld_json) string in the request.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          201 - Created           => headers is a list of headers to return to the client. It should contain at least a location entry with the URL
                                     of the newly-created resource. If no content_type header is given, it will be set to 'application/rdf+json+ce'
                                     body may be an empty list or a dictionary that contains the ld+json representaton of the created object
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """
        status, headers, container = self.recursive_get_document(query_string=self.query_string+'?non-member-properties' if self.query_string else 'non-member-properties')
        if status == 200:
            return self.insert_document(container, document, document_id)
        else:
            return status, headers, container

    def insert_document(self, container, document, document_id=None):
        if CHECK_ACCESS_RIGHTS:
            status, permissions = self.permissions(container, document)
            if status == 200:
                if not permissions & AC_C:
                    return 403, [], [('', 'not authorized')]
            else:
                return 403, [], [('', 'unable to retrieve permissions. status: %s text: %s' % (status, permissions))]
        document = rdf_json.RDF_JSON_Document(document, '')
        self.complete_document_for_container_insertion(document, container)
        self.complete_document_for_storage_insertion(document)
        self.preprocess_properties_for_storage_insertion(document)
        status, location, result = operation_primitives.create_document(self.user, document, self.request_hostname, self.tenant, self.namespace, document_id)
        if status == 201:
            if self.change_tracking:
                self.generate_change_event(CREATION_EVENT, location)
            # Todo: fix up self.document_id, self.path, self.path_parts to match location url of new document
            self.complete_result_document(result)
            return status, [('Location', str(location))], result
        else:
            return status, [], [('', result)]

    def put_document(self, document):
        return 405, [], [('', 'PUT not allowed')]

    def execute_query(self, query):
        """
        Execute the specified query.

        Queries are safe and idempotent. That is, they do not have side-effects, and (weaker and implied by safe) the result of doing them
        muultiple times is the same as doing them once. In that sense, they are similar to a GET, but done via POST.

        The 'query' argument is a Python dictionary that was created from a json string in the request. The format of the JSON will
        depend on the database back-end.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          200 - OK                => headers is a list of headers to return to the client. If no content_type header is given, it will be set to
                                     'application/rdf+json+ce'
                                     body may be an empty list or a dictionary that contains the json representaton of the query result
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """
        if not self.namespace or self.document_id: #trailing / or other problem
            return self.bad_path()
        status, result = operation_primitives.execute_query(self.user, query, self.request_hostname, self.tenant, self.namespace)
        return status, [], result

    def execute_action(self, body):
        """
        This method is called when a POST is made that means 'execute action'.

        The 'body' argument is a Python dictionary that was created from a json string in the request. The format of the JSON will
        depend on the action.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          200 - OK                => headers is a list of headers to return to the client. If no content_type header is given, it will be set to
                                     'application/rdf+json+ce'
                                     body may be an empty list or a dictionary that contains the json representaton of the query result
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """
        return 400, [], [('', 'unknown action')]

    def permissions(self, document, insert_document=None):
        owner = document.get_value(CE+'owner')
        if self.user == str(owner):
            return 200, AC_ALL # owner can do everything
        else:
            resource_group = document.get_value(AC+'resource-group')
            if resource_group:
                permissions_url = url_policy.construct_url(self.request_hostname, self.tenant, 'ac-permissions') + ('?%s&%s' % (urllib.quote(str(resource_group)), urllib.quote(self.user)))
                r = self.intra_system_get(permissions_url)
                if r.status_code == 200:
                    return 200, int(r.text)
                else:
                    return r.status_code, 'url: %s text: %s' % (permissions_url, r.text)
        return 200, 0

    def resource_groups(self):
        resource_group_url = url_policy.construct_url(self.request_hostname, self.tenant, 'ac-resource-groups') + ('?%s' % urllib.quote(self.user))
        r = self.intra_system_get(resource_group_url)
        if r.status_code == 200:
            return json.loads(r.text, object_hook=rdf_json.rdf_json_decoder)
        else:
            return []

    def get_health(self):
        # Before claiming to be healthy, Make sure that we can do outgoing intra_system calls
        intra_system_test_url = url_policy.construct_url(self.request_hostname, self.tenant, 'favicon.ico')
        try:
            with utils.LIMIT_LOGGING_LEVEL_INFO:
                r = self.intra_system_get(intra_system_test_url)
            if r.status_code != 200 and r.status_code != 404:
                # Note that 404 means that we are able to reach the SYSTEM_HOST, so we're healthy, even if it doesn't implement favicon.ico
                return r.status_code, [], [('','intra_system_get not functioning: %s' % r.text)]
        except ConnectionError as e:
            return 504, [], [('','intra_system_get exception: %s' % e.message)]
        return 200, [('Content-Type', 'text/plain'), ('Content-length', '1')], ['1']

    def get_document(self):
        """
        GET the document associated with 'self'.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          200 - OK                => headers is a list of headers to return to the client. If no content_type header is given, it will be set to
                                     'application/rdf+json+ce'
                                     body is a dictionary that contains the json (or ld+json) representaton of the resource
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """
        if self.document_id is None:
            return self.get_collection()
        if not self.namespace:
            return 404, [], [('', 'no resource with the URL: %s' % self.request_url())]
        status, document = operation_primitives.get_document(self.user, self.request_hostname, self.tenant, self.namespace, self.document_id)
        if status == 200:
            # we found the document, but is the user entitled to see it?
            if CHECK_ACCESS_RIGHTS:
                status, permissions = self.permissions(document)
                if status == 200:
                    if not permissions & AC_R:
                        return 403, [], [('', 'not authorized')]
                else:
                    return 403, [], [('', 'unable to retrieve permissions. status: %s text: %s' % (status, permissions))]
            status, document = self.complete_request_document(document)
            return status, [], document
        else:
            return status, [], [('', document)]

    def get_collection(self):
        """
        This method returns a storage collection as a Basic Profile Container.
        TODO: Need to support paging for large collections
        """
        if not self.namespace: # nope, not a pre-existing container resource either
            return self.bad_path()
        # TODO: What access control specs govern these "built-in" collections? Who can see them? What resource-group are they part of?
        container_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace)
        container_properties = { RDF+'type': URI(LDP+'DirectContainer'),
                                 LDP+'membershipResource': URI(container_url),
                                 LDP+'hasMemberRelation': URI(LDP+'member'),
                                 CE+'owner': URI(ADMIN_USER),
                                 AC+'resource-group': self.default_resource_group() }
        document = rdf_json.RDF_JSON_Document({ container_url : container_properties }, container_url)
        if self.query_string.endswith('non-member-properties'):
            document.default_subject_url = document.graph_url
            document.graph_url = document.graph_url + '?non-member-properties'
            status = 200
        else:
            status, results = operation_primitives.execute_query(self.user, {}, self.request_hostname, self.tenant, self.namespace)
            if status == 200:
                self.add_member_detail(document, results)
                member_values = []
                for result in results:
                    member_values.append(URI(result.graph_url))
                if len(member_values) != 0:
                    container_properties[LDP+'member'] = member_values
                    container_properties[LDP+'contains'] = member_values
            else:
                return status, [], [('', results)]
        return status, [], document

    def delete_document(self):
        """
        DELETE the document associated with 'self'.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          204 - No content        => Successful delete. Headers is an optional list of headers to return to the client.
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """ 
        resource_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id)
        document = self.get_document()[2]
        if CHECK_ACCESS_RIGHTS:
            status, permissions = self.permissions(document)
            if status == 200:
                if not permissions & AC_C:
                    return 403, [], [('', 'not authorized')]
            else:
                return 403, [], [('', 'unable to retrieve permissions. status: %s text: %s' % (status, permissions))]
        if self.document_id is None:
            return self.drop_collection()
        if not self.namespace: #trailing / or other problem
            return self.bad_path()
        status, err_msg = operation_primitives.delete_document(self.user, self.request_hostname, self.tenant, self.namespace, self.document_id)
        if self.change_tracking:
            resource_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id)
            self.generate_change_event(DELETION_EVENT, resource_url)
        return status, [], [('', err_msg)] if err_msg else []

    def drop_collection(self):
        if not self.namespace: # nope, not a pre-existing container resource either
            return self.bad_path()
        operation_primitives.drop_collection(self.user, self.request_hostname, self.tenant, self.namespace)
        operation_primitives.drop_collection(self.user, self.request_hostname, self.tenant, self.namespace + '_history')
        operation_primitives.drop_collection(self.user, self.request_hostname, self.tenant, self.namespace + '_tracking')
        document_namespace = self.tenant + '/' + self.namespace
        if self.change_tracking and document_namespace in self.trs_builders:
            del self.trs_builders[document_namespace]
        return 204, [], []

    def patch_document(self, request_body):
        """
        PATCH the contents of document associated with 'self'.

        The 'request_body' argument is a Python dictionary that was created from a json string in the request. The format of the JSON will
        depend on the database back-end.

        The return value is a triple of (status, headers, body). The values of headers and body depends on the status:
          200 - OK                => Successful patch. Headers is an optional list of headers to return to the client.
                                     body is a dictionary that may contain the ld+json representaton of the patched resource
          others                  => headers may be an empty list or may optionally include headers to return to the client
                                     body should be a list of pairs, where the first element of the pair identifies the field in error, or is ''.
                                     The second element of the pair should start with a number, a space, and an optional string explaining the error
        """
        if not self.namespace: #trailing / or other problem
            return self.bad_path()
        resource_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id)
        document = rdf_json.RDF_JSON_Document(request_body, resource_url)
        if CHECK_ACCESS_RIGHTS:
            prepatch_document = self.get_document()[2]
            status, permissions = self.permissions(prepatch_document)
            if status == 200:
                if not permissions & AC_C:
                    return 403, [], [('', 'not authorized')]
            else:
                return 403, [], [('', 'unable to retrieve permissions. status: %s text: %s' % (status, permissions))]
        if not 'HTTP_CE_MODIFICATIONCOUNT' in self.environ:
            return 400, [], [('', 'Must provide CE-ModificationCount header')]
        try:
            mod_count = int(self.environ['HTTP_CE_MODIFICATIONCOUNT'])
        except ValueError:
            return 400, [], [('', 'CE-ModificationCount header must be an integer: %s' % self.environ['HTTP_CE-MODIFICATIONCOUNT'])]
        self.preprocess_properties_for_storage_insertion(document)
        status, result = operation_primitives.patch_document(self.user, mod_count, request_body, self.request_hostname, self.tenant, self.namespace, self.document_id)   
        if(status == 200):
            get_status, headers, new_document = self.get_document()
            if(get_status == 200):
                if self.change_tracking:
                    self.generate_change_event(MODIFICATION_EVENT, resource_url)
                return 200, headers, new_document
            else:
                return get_status, [], [('', 'Patch was successful but getting the document afterwards failed')]
        else:
            return status, [], [('', result)]

    def document_url(self):
        return url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id)

    def absolute_url(self, relative_url):
        return urlparse.urljoin(self.request_url(), relative_url)

    def request_url(self):
        return url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id, self.extra_path_segments, self.query_string)

    def add_member_detail(self, container, result):
        for rdf_json_document in result:
            # we will include the membership triples, plus any triples in the same documents. This will pick up the triples that describe the members.
            for subject, subject_node in rdf_json_document.iteritems():
                #warning - tricky code. If a membership subject is set to the collection, the member documents will contain triples whose subject is the container itself.
                #To avoid infinite loops, we must not call complete_result_document on this subject. To avoid this, we see if the subject is already in the result
                new_subject = subject not in container
                for predicate, value_array in subject_node.iteritems():
                    container.add_triples(subject, predicate, value_array)
                if new_subject:
                    self.complete_result_document(rdf_json.RDF_JSON_Document(container.data, subject))

    def add_bpc_member_properties(self, container, query=None):
        ldp_resource = container.get_value(LDP+'membershipResource')
        ldp_hasMember = container.get_value(LDP+'hasMemberRelation')
        ldp_isMemberOf = container.get_value(LDP+'isMemberOfRelation')
        ldp_containerSortPredicate = container.get_value(CE+'containerSortPredicates')
        if not ldp_resource:
            raise ValueError('must provide a membership resource')
        elif ldp_hasMember:
            if ldp_isMemberOf: raise ValueError('cannot provide both hasMember and isMemberOf predicates')
            if not query:
                query = {str(ldp_resource) : {str(ldp_hasMember) : '_any'}}
        elif ldp_isMemberOf: # subject or object may be set, but not both
            if ldp_hasMember: raise ValueError('cannot provide both hasMember and isMemberOf predicates')
            if not query:
                if ldp_resource == '_any':
                    query = {'_any': {str(ldp_isMemberOf) : '_any'}}
                else:
                    query = {'_any': {str(ldp_isMemberOf) : ldp_resource}}
        else:
            return 200, container
        if CHECK_ACCESS_RIGHTS:
            resource_groups = self.resource_groups()
            query['_any2'] = {}
            if len(resource_groups) > 0:
                if len(resource_groups) > 1:
                    resource_group_value = {'$in': resource_groups}
                else:
                    resource_group_value = resource_groups[0]
                query['_any2']['$or'] = [{CE+'owner': URI(self.user)}, {AC+'resource-group': resource_group_value}]
            else:
                query['_any2'][CE+'owner'] = URI(self.user)
        if ldp_containerSortPredicate:
            query = {'$query': query, '$orderby' : {ldp_containerSortPredicate: 1}}
        status, result = operation_primitives.execute_query(self.user, query, self.request_hostname, self.tenant, self.namespace)
        if status == 200:
            self.add_member_detail(container, result)
            return 200, container
        else:
            return status, [('', result)]

    def complete_container(self, document):
        if self.query_string.endswith('non-member-properties'):
            document.default_subject_url = document.graph_url
            document.graph_url = document.graph_url + '?non-member-properties'
            return 200, document
        else:
            status, document = self.add_bpc_member_properties(document)
            if status == 200:
                members = document.get_container_members()
                if len(members) > 0:
                    document.set_value(LDP+'contains', members)
            return status, document

    def complete_result_document(self, document):
        return 200, document

    def complete_request_document(self, document):
        self.complete_result_document(document) # will add any calculated properties, including owned containers.
        document_url = document.graph_url #self.document_url()
        if self.extra_path_segments == None: # a simple document URL with no extra path segments
            document.add_triples(document_url, CE+'allVersions', URI('/'.join((document_url, 'allVersions'))))
        else:
            if len(self.extra_path_segments) == 1 and self.extra_path_segments[0] == 'allVersions' and not self.query_string: # client wants history collection
                status, document = self.create_all_versions_container(document)
                return status, document
        request_url = self.request_url()
        if document.graph_url != request_url: #usually a bad thing, unless it's an owned container that was being asked for
            owned_container_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id, self.extra_path_segments)
            if owned_container_url in document.data and URI(LDP+'DirectContainer') in document.get_values(RDF+'type', owned_container_url):
                document.graph_url = owned_container_url
                return self.complete_container(document)
        if URI(LDP+'DirectContainer') in document.get_values(RDF+'type'):
            status, document = self.complete_container(document)
        else:
            status = 200
        if document.graph_url != self.request_url():
            return 404, [('', 'no document matching that url: %s , graph_url: %s' % (self.request_url(), document.graph_url))]
        else:
            return status, document

    def complete_document_for_container_insertion(self, document, container):
        ldp_resource = container.get_value(LDP+'membershipResource')
        ldp_hasMember = container.get_value(LDP+'hasMemberRelation')
        ldp_isMemberOf = container.get_value(LDP+'isMemberOfRelation')
        if not ldp_resource:
            raise ValueError('must provide container resource: %s' % container)
        elif ldp_hasMember:
            if ldp_isMemberOf: raise ValueError('cannot provide both hasMember and isMemberOf predicates: %s' % container)
            # store the membership triple in the new document
            document.add_triples(ldp_resource, ldp_hasMember, URI('')) # last argument is null relative address of resource-to-be
        elif ldp_isMemberOf:
            if ldp_hasMember: raise ValueError('cannot provide both hasMember and isMemberOf predicates: %s' % container)
            # store the membership triple in the new document
            document.add_triple('', ldp_isMemberOf, ldp_resource) # first argument is null relative address of resource-to-be
        else:
            raise ValueError('must provide a membership predicate')

    def complete_document_for_storage_insertion(self, document):
        document.set_value(CE+'owner', URI(self.user))
        if document.get_value(AC+'resource-group') is None:
            default_resource_group = self.default_resource_group()
            if default_resource_group:
                document.set_value(AC+'resource-group', default_resource_group)

    def preprocess_properties_for_storage_insertion(self, rdf_json):
        pass

    def default_resource_group(self):
        return URI(url_policy.construct_url(self.request_hostname, self.tenant)) # default is the root resource (i.e., '/')

    def add_container(self, document, container_url, membership_resource, membership_predicate, member_is_object=False, container_resource_group=None, container_owner=None) :
        if container_resource_group is None:
            container_resource_group = self.default_resource_group()
        document[container_url] = {
                RDF+'type': URI((LDP+'DirectContainer')),
                LDP+'membershipResource' : URI(membership_resource),
                (LDP+'hasMemberRelation' if member_is_object else LDP+'isMemberOfRelation') : URI(membership_predicate),
                AC+'resource-group' : container_resource_group
                }
        if container_owner is not None:
            document[container_url][CE+'owner'] = container_owner

    def create_container(self, container_url, membership_resource, membership_predicate, member_is_object=False):
        document = rdf_json.RDF_JSON_Document ({}, container_url)
        self.add_container(document, container_url, membership_resource, membership_predicate, member_is_object, None, None)
        return document

    def container_from_membership_resource_in_query_string(self, membership_predicate, member_is_object=False):
        if self.query_string.endswith('?non-member-properties'):
            qs = self.query_string[:-22]
        else:
            qs = self.query_string
        container_url = url_policy.construct_url(self.request_hostname, self.tenant, self.namespace, self.document_id, self.extra_path_segments, qs)
        membership_resource = self.absolute_url(urllib.unquote(qs))
        document = self.create_container(container_url, membership_resource, membership_predicate, member_is_object)
        status, document = self.complete_result_document(document)
        return status, [], document

    def query_resource_document(self, membership_resource, membership_predicate, make_result, member_is_object=False):
        if member_is_object:
            query = {str(membership_resource) : {str(membership_predicate) : '_any'}}
        else:
            query = {'_any': {str(membership_predicate) : URI(membership_resource)}}
        status, result = operation_primitives.execute_query(self.user, query, self.request_hostname, self.tenant, self.namespace)
        if status == 200:
            if len(result) == 0:
                return 404, [], [('', '404 error - no such virtual document %s' % query)]
            elif len(result) == 1:
                return make_result(result)
            else:
                return 404, [], [('', '404 error - ambiguous virtual document - should be a LDPC collection?')]
        else:
            return status, [], [('', result)]

    def resource_from_membership_info(self, membership_resource, membership_predicate, member_is_object=False):
        def make_result(result):
            document = result[0]
            document.add_triples(self.request_url(), OWL+'sameAs', document.graph_url)
            self.complete_result_document(document)
            return 200, [('Content-Location', str(document.graph_url))], document
        return self.query_resource_document(membership_resource, membership_predicate, make_result, member_is_object)

    def resource_from_membershipResource_in_query_string(self, membership_predicate, member_is_object=False):
        membership_resource = self.absolute_url(urllib.unquote(self.query_string))
        return self.resource_from_membership_info(membership_resource, membership_predicate, member_is_object)

    def resource_from_object_in_query_string(self, membership_predicate, member_is_object=False):
        print 'resource_from_object_in_query_string is deprecated - use resource_from_membershipResource_in_query_string'
        return self.resource_from_membershipResource_in_query_string(membership_predicate, member_is_object)

    def add_resource_triples(self, document, membership_resource, membership_predicate, member_is_object=False):
        def make_result(result):
            self.add_member_detail(document, result)
            return 200, [], document
        return self.query_resource_document(membership_resource, membership_predicate, make_result, member_is_object)

    def add_owned_container(self, document, container_predicate, container_path_segment, membership_predicate, member_is_object=False):
        document_url = document.graph_url
        document.add_triples(document_url, container_predicate, URI(document_url + '/' + container_path_segment))
        if self.request_url().startswith(document_url) and self.extra_path_segments != None and len(self.extra_path_segments) == 1 and self.extra_path_segments[0] == container_path_segment:
            # client doesn't really want the document, just its owned container
            container_resouce_group = document.get_value(AC+'resource-group')
            container_owner = document.get_value(CE+'owner')
            container_graph_url = document_url + '/' + container_path_segment
            self.add_container(document, container_graph_url, document_url, membership_predicate, member_is_object, container_resouce_group, container_owner)

    def add_inverse(self, document, property_predicate, membership_shortname, namespace=None):
        if not namespace:
            namespace = self.namespace
        #FB query_string = urllib.quote(self.document_url())
        #FB  GET http%3A//localhost%3A5001/xdo/webserver/deployments generates:
        #FB      ce_group: http://localhost:5001/sx/ce_for_deployment?http%3A//localhost%3A5001/xdo/webserver
        #FB  instead of:
        #FB      ce_group: http://localhost:5001/sx/ce_for_deployment?http%3A//localhost%3A5001/xdo/webserver_v1
        query_string = urllib.quote(document.graph_url)
        url = url_policy.construct_url(self.request_hostname, self.tenant, namespace, membership_shortname, query_string=query_string)
        document.set_value(property_predicate, URI(url))

    def create_all_versions_container(self, document):
        history = document.get_values(HISTORY)
        status, query_result = operation_primitives.get_prior_versions(self.user, self.request_hostname, self.namespace, history)
        if status == 200:
            request_url = self.request_url() # the url of the allVersions collection
            result_document = rdf_json.RDF_JSON_Document ({}, request_url)
            result_document[request_url] = {
                '#id' : URI('all versions'),
                RDF+'type': URI(LDP+'DirectContainer'),
                LDP+'membershipResource' : URI(document.graph_url),
                LDP+'isMemberOfRelation' : URI(CE+'versionOf')
                }
            result_document.add_triples(document.graph_url, CE+'versionOf', URI(document.graph_url))
            result_document.add_triples(document.graph_url, CE+'graph', [{'type': 'graph', 'value': document}])
            for version in query_result:
                result_document.add_triples(version.graph_url, CE+'versionOf', URI(document.graph_url))
                result_document.add_triples(version.graph_url, CE+'graph', [{'type': 'graph', 'value': version}])
                document_url = version.get_value(CE+'versionOf')
                del version[version.graph_url]
                version.default_subject_url = document_url
            return 200, result_document
        else:
            return status, query_result

    def generate_change_event(self, event_type, resource_uri):
        document_namespace = self.tenant + '/' + self.namespace #Todo: - do this better
        if document_namespace not in self.trs_builders:
            self.trs_builders[document_namespace] = TrackedResourceSetBuilder(self.request_hostname, document_namespace)
        self.trs_builders[document_namespace].addChangeEntry(resource_uri, event_type)

    def namespace_mappings(self):
        return NAMESPACE_MAPPINGS

    def convert_rdf_json_to_compact_json(self, document):
        converter = rdf_json.RDF_json_to_compact_json_converter(self.namespace_mappings())
        compact_json = converter.convert_to_compact_json(document)
        return compact_json

    def convert_rdf_json_to_html(self, document):
        from example_rdf_json_to_html_converter import Rdf_json_to_html_converter
        return Rdf_json_to_html_converter().convert_rdf_json_to_html(document)

    def bad_path(self):
        return 400, [], [('', '4001 - bad path: %s (trailing / or path too short or other problem)' % self.path)]

    def check_input_value(self, rdf_document, predicate, field_errors, value_type=None, required=True, subject=None, expected_value=None):
        return rdf_document.check_value(predicate, field_errors, value_type, required, subject, expected_value)

    def intra_system_get(self, request_url, headers=None):
        if not headers: headers = dict()
        actual_url = utils.set_resource_host_header(str(request_url), headers)
        if not 'SSSESSIONID' in headers:
            headers['SSSESSIONID'] = utils.get_jwt(self.environ)
        if not 'Accept' in headers:
            headers['Accept'] = 'application/rdf+json+ce'
        logger.debug('intra_system_get request_url: %s actual_url: %s headers: %s', request_url, actual_url, headers)
        return requests.get(actual_url, headers=headers)

    def intra_system_post(self, request_url, data, headers=None):
        if not headers: headers = dict()
        if not 'SSSESSIONID' in headers:
            headers['SSSESSIONID'] = utils.get_jwt(self.environ)
        if not 'Content-Type' in headers:
            headers['Content-Type'] = 'application/rdf+json+ce'
        if not 'CE-Post-Reason' in headers:
            headers['CE-Post-Reason'] = 'CE-Create'
        actual_url = utils.set_resource_host_header(str(request_url), headers)
        logger.debug('intra_system_post request_url: %s actual_url: %s headers: %s data: %s', request_url, actual_url, headers,data)
        return requests.post(actual_url, headers=headers, data=json.dumps(data, cls=rdf_json.RDF_JSON_Encoder), verify=False)
        
    def intra_system_patch(self, request_url, modification_count, data, headers=None):
        if not headers: headers = dict()
        if not 'SSSESSIONID' in headers:
            headers['SSSESSIONID'] = utils.get_jwt(self.environ)
        if not 'Content-Type' in headers:
            headers['Content-Type'] = 'application/rdf+json+ce'
        headers['CE-ModificationCount'] = str(modification_count)
        actual_url = utils.set_resource_host_header(str(request_url), headers)
        logger.debug('intra_system_patch request_url: %s actual_url: %s headers: %s data: %s', request_url, actual_url, headers,data)
        return requests.patch(actual_url, headers=headers, data=json.dumps(data, cls=rdf_json.RDF_JSON_Encoder), verify=False)

    def intra_system_delete(self, request_url, headers=None):
        if not headers: headers = dict()
        if not 'SSSESSIONID' in headers:
            headers['SSSESSIONID'] = utils.get_jwt(self.environ)
        actual_url = utils.set_resource_host_header(str(request_url), headers)
        logger.debug('intra_system_delete request_url: %s actual_url: %s headers: %s', request_url, actual_url, headers)
        return requests.delete(actual_url, headers=headers, verify=False)
        
    def intra_system_put(self, request_url, data, headers=None):
        if not headers: headers = dict()
        if not 'SSSESSIONID' in headers:
            headers['SSSESSIONID'] = utils.get_jwt(self.environ)
        if not 'Content-Type' in headers:
            headers['Content-Type'] = 'application/rdf+json+ce'
        actual_url = utils.set_resource_host_header(str(request_url), headers)
        logger.debug('intra_system_put request_url: %s actual_url: %s headers: %s data: %s', request_url, actual_url, headers,data)
        return requests.put(actual_url, headers=headers,  data=json.dumps(data, cls=rdf_json.RDF_JSON_Encoder), verify=False)

def get_header(header, headers, default=None):
    headerl = header.lower()
    for item in headers:
        if item[0].lower() == headerl:
            return item[1]
    return default
