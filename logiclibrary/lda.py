# This module provides helper functions for lda server implementations.
#
import os
import json
import Cookie, jwt
from webob import Request

if 'URL_POLICY_CLASS' not in os.environ:
    os.environ['URL_POLICY_CLASS'] = 'url_policy#TypeQualifiedHostnameTenantURLPolicy'
    
import rdf_json
import example_logic_tier as base
from rdfgraphlib import rdfjson_to_graph, serialize_graph
from storage import operation_primitives
from base_constants import RDF, LDP

def create_document(environ, document, membership_property, complete_document_callback=None):
    domain_logic = Domain_Logic(environ, complete_document_callback)
    container_url = domain_logic.document_url()
    document[membership_property] = container_url
    document = domain_logic.convert_compact_json_to_rdf_json(document)
    status, headers, body = domain_logic.create_document(document, domain_logic.document_id + '/')
    if not hasattr(body, 'graph_url'): # not an rdf_json document - probably an error condition
        body = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
    else:
        body = domain_logic.convert_rdf_json_to_compact_json(body)
    return body, status, headers

def get_document(environ, complete_document_callback=None):
    domain_logic = Domain_Logic(environ, complete_document_callback)
    status, headers, body = domain_logic.get_document()
    add_standard_headers(environ, headers)
    if not hasattr(body, 'graph_url'): # not an rdf_json document - probably an error condition
        body = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
    else:
        body = domain_logic.convert_rdf_json_to_compact_json(body)
    return body, status, headers

def patch_document(environ, document, complete_document_callback=None):
    domain_logic = Domain_Logic(environ, complete_document_callback)
    document = domain_logic.convert_compact_json_to_rdf_json(document)
    status, headers, body = domain_logic.patch_document(document)
    if not hasattr(body, 'graph_url'): # not an rdf_json document - probably an error condition
        body = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
    else:
        body = domain_logic.convert_rdf_json_to_compact_json(body)
    return body, status, headers

def delete_document(environ):
    domain_logic = Domain_Logic(environ)
    status, headers, body = domain_logic.delete_document()
    add_standard_headers(environ, headers)
    return body, status, headers

def get_virtual_container(environ, membership_property, complete_document_callback=None):
    domain_logic = Domain_Logic(environ, complete_document_callback)
    container_url = domain_logic.document_url()
    converter = rdf_json.Compact_json_to_rdf_json_converter(domain_logic.namespace_mappings())
    membership_predicate = converter.expand_predicate(membership_property)
    body = domain_logic.create_container(container_url, container_url, membership_predicate) 
    status, body = domain_logic.complete_request_document(body)
    if not hasattr(body, 'graph_url'): # not an rdf_json document - probably an error condition
        body = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
    else:
        body = domain_logic.convert_rdf_json_to_compact_json(body)
    return body, status, []

def execute_query(environ, query, membership_property, complete_document_callback=None):
    domain_logic = Domain_Logic(environ, complete_document_callback)
    #query = domain_logic.convert_compact_json_to_rdf_json(query)
    status, headers, result = domain_logic.execute_query(query)
    if status == 200:
        container_url = domain_logic.request_url()
        container_predicates = {
            RDF+'type': rdf_json.URI(LDP+'BasicContainer'),
            LDP+'contains': [rdf_json.URI(resource.default_subject()) for resource in result]
        }
        document = rdf_json.RDF_JSON_Document({container_url: container_predicates}, container_url)
        domain_logic.add_member_detail(document, result)
        body = domain_logic.convert_rdf_json_to_compact_json(document)
    else:
        body = json.dumps(result, cls=rdf_json.RDF_JSON_Encoder)
    return body, status, headers

def convert_to_requested_format(document, headers, environ): #TODO: pass in req, instead of environ ???
    # In this application architectural style, the only method that ever returns HTML is GET. We never
    # return HTML from POST and we do not support application/x-www-form-urlencoded for POST
    domain_logic = Domain_Logic(environ)
    #TODO: if there is no accept header then use content-type header for post response ... is that what best_match does already?
    req = Request(environ)
    best_match = req.accept.best_match(['application/json', # default
                                        'text/html',
                                        'application/rdf+json',
                                        'application/rdf+json+ce',
                                        'application/rdf+xml',
                                        'text/turtle',
                                        'application/x-turtle',
                                        'application/ld+json'])
    if best_match == 'application/json':
        body = json.dumps(document)
    else:
        document = domain_logic.convert_compact_json_to_rdf_json(document)
        if best_match == 'application/rdf+json+ce':
            body = json.dumps(document, cls=rdf_json.RDF_JSON_Encoder)
        elif best_match == 'application/rdf+json':
            document = rdf_json.normalize(document)
            body = json.dumps(document, cls=rdf_json.RDF_JSON_Encoder)
        elif best_match == 'application/rdf+xml' or best_match == 'text/turtle' or best_match == 'application/x-turtle' or best_match == 'application/ld+json':
            graph = rdfjson_to_graph(rdf_json.normalize(document))
            body = serialize_graph(graph, best_match, None) #TODO: should we use wfile instead of string return value?
        elif best_match == 'text/html':
            body = domain_logic.convert_rdf_json_to_html(document)
    if not header_set('Content-Type', headers):
        headers.append(('Content-Type', best_match))
    if not header_set('Cache-Control', headers):
        headers.append(('Cache-Control', 'no-cache'))
    headers.append(('Content-length', str(len(body))))
    return body, headers
        
def add_standard_headers(environ, headers):
    origin = environ.get('HTTP_ORIGIN')
    if origin and not header_set('Access-Control-Allow-Origin', headers):
        headers.append(('Access-Control-Allow-Origin', origin))
        headers.append(('Access-Control-Allow-Credentials', 'true'))
        headers.append(('Access-Control-Expose-Headers', 'Content-Location, Location'))
    if ('HTTP_AUTHORIZATION' in environ and environ['HTTP_AUTHORIZATION'].lower().startswith('bearer ')):
        # user credentials from another domain were passed by the client
        session_key = environ['HTTP_AUTHORIZATION'][len('bearer '):]
        add_cookie = True
        cookie = Cookie.SimpleCookie()
        if ('HTTP_COOKIE' in environ):
            cookie.load(environ['HTTP_COOKIE'])
            if 'SSSESSIONID' in cookie:
                add_cookie = False
    elif ('GUEST_AUTHORIZATION' in environ):
        #  a JWT for an anonymous user URL was generated for an unauthenticated request or the JWT claims expired
        session_key = environ['GUEST_AUTHORIZATION']
        add_cookie = True
    else:
        add_cookie = False
    if add_cookie:
        cookie = Cookie.SimpleCookie()
        cookie['SSSESSIONID'] = session_key # SSSESSIONID  is 'Site Server Session ID'
        cookie['SSSESSIONID']['path'] = '/'
        claims = jwt.decode(session_key, verify=False)
        cookie['user'] = claims['user']
        cookie['user']['path'] = '/'
        cookie_headers = map(lambda morsel: ('Set-Cookie', morsel.OutputString()), cookie.values())
        headers.extend(cookie_headers)

def header_set(header, headers):
    headerl = header.lower()
    for item in headers:
        if item[0].lower() == headerl:
            return True
    return False

class Domain_Logic(base.Domain_Logic):
    def __init__(self, environ, complete_document_callback=None, change_tracking=False):
        self.complete_document_callback = complete_document_callback
        super(Domain_Logic, self).__init__(environ, change_tracking)

    def create_document(self, document, document_id):
        # TODO: access control checking
        document = rdf_json.RDF_JSON_Document(document, '')
        self.complete_document_for_storage_insertion(document)
        self.preprocess_properties_for_storage_insertion(document)
        status, location, result = operation_primitives.create_document(self.user, document, self.request_hostname, self.tenant, self.namespace, document_id)
        if status == 201:
            if self.change_tracking:
                self.generate_change_event(base.CREATION_EVENT, location)
            # Todo: fix up self.document_id, self.path, self.path_parts to match location url of new document
            self.complete_result_document(result)
            return status, [('Location', str(location))], result
        else:
            return status, [], [('', result)]

    def execute_query(self, query):
        if not self.namespace:
            return self.bad_path()
        status, result = operation_primitives.execute_query(self.user, query, self.request_hostname, self.tenant, self.namespace)
        return status, [], result

    def complete_result_document(self, document):
        rdf_type = document.get_value(RDF+'type')
        if rdf_type and not str(rdf_type).startswith(LDP):
            if self.complete_document_callback is not None:
                temp_doc = self.convert_rdf_json_to_compact_json(document)
                self.complete_document_callback(temp_doc)
                #document.clear()
                document.update(self.convert_compact_json_to_rdf_json(temp_doc))
        return super(Domain_Logic, self).complete_result_document(document)
