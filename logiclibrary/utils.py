import os
import Cookie, cryptography
import uuid
import requests, urlparse
import json
from base_constants import ADMIN_USER
from rdf_json import RDF_JSON_Encoder

if 'HOSTINGSITE_HOST' in os.environ:
    HOSTINGSITE_HOST = os.environ['HOSTINGSITE_HOST'].lower() # hostname and port (if there is one)
    HOSTINGSITE_HOST = HOSTINGSITE_HOST if len(HOSTINGSITE_HOST.split(':')) > 1 else HOSTINGSITE_HOST+':80'
else:
    HOSTINGSITE_HOST = None
    
SYSTEM_HOST = os.environ.get('SYSTEM_HOST') if 'SYSTEM_HOST' in os.environ else None

def construct_url(hostname, tenant, namespace=None, document_id = None, extra_segments = None, query_string = None):
    #hostname is the request hostname. If the hostname is null we are building a 
    #relative url. The caller is responsible
    #for assuring that the hostname is compatible with the tenant.
    
    if document_id is not None:
        parts = ['http:/', hostname, namespace, document_id] if hostname is not None else ['', namespace, document_id]
        if extra_segments is not None:
            parts.extend(extra_segments)
    else:
        if extra_segments is not None:
            raise ValueError
        if namespace is not None:
            parts = ['http:/', hostname, namespace] if hostname is not None else ['', namespace]
        else:
            parts = ['http:/', hostname, ''] if hostname is not None else ['','']
    result =  '/'.join(parts)
    if query_string:
        return '?'.join((result, query_string))
    else:
        return result
 
def get_url_components(environ): 
    path = environ['PATH_INFO']
    path_parts = path_parts = path.split('/')
    namespace = document_id = extra_path_segments = None
    ce_resource_host = environ.get('HTTP_CE_RESOURCE_HOST')
    request_host = ce_resource_host.lower() if ce_resource_host else environ['HTTP_HOST'].lower()
    request_host = request_host if len(request_host.split(':')) > 1 else request_host+':80'
    if HOSTINGSITE_HOST is None or request_host == HOSTINGSITE_HOST: 
        tenant = 'hostingsite'
    else:
        tenant_parts = request_host.split('.')
        if '.'.join(tenant_parts[1:]) == HOSTINGSITE_HOST:
            tenant = tenant_parts[0]
        else:
            #TODO: look up a table to see if it's a 'custom domain' for a known tenant
            tenant = None
    if len(path_parts) > 1 and path_parts[-1] != '': #trailing /
        namespace = path_parts[1]
        if len(path_parts) > 2:
            document_id = path_parts[2]
            if len(path_parts) > 3:
                extra_path_segments = path_parts[3:]
    return (tenant, namespace, document_id, extra_path_segments, path, path_parts, get_request_host(environ), environ['QUERY_STRING'])

def get_jwt(environ):
    session_key = None
    if ('HTTP_COOKIE' in environ):
        cookie = Cookie.SimpleCookie()
        cookie.load(environ['HTTP_COOKIE'])
        if 'SSSESSIONID' in cookie:
            session_key = cookie['SSSESSIONID'].value
    elif ('HTTP_SSSESSIONID' in environ):
        session_key = environ['HTTP_SSSESSIONID']
    elif ('SSSESSIONID' in environ):
        session_key = environ['SSSESSIONID']
    return session_key

def get_claims(environ):
    session_key = get_jwt(environ)
    if session_key:
        return cryptography.decode_jwt(session_key) 
    else:
        return None       
        
def get_or_create_claims(environ):
    jwt = get_jwt(environ)
    if jwt:
        claims = cryptography.decode_jwt(jwt) 
        if not claims: # expired claims?
            claims = cryptography.decode_jwt(jwt, verify_expiration=False)
            if claims: # we have a verified set of claims, but they have expired
                del claims['acc']
                del claims['exp']
                environ['SSSESSIONID'] = cryptography.encode_jwt(claims)
    else:
        claims = None
    if not claims:
        claims = create_anonymous_user_claims(environ)
        environ['SSSESSIONID'] = cryptography.encode_jwt(claims)
    return claims
        
def create_anonymous_user_claims(environ):
    host = get_request_host(environ)
    anonymous_user = 'http://%s/unknown_user/%s' % (host, uuid.uuid4())
    return {'user': anonymous_user} 

def get_request_host(environ):
    return environ.get('HTTP_CE_RESOURCE_HOST') or environ['HTTP_HOST']

def prepare_intra_system_call(request_url, headers):
    if SYSTEM_HOST is not None:
        parts = list(urlparse.urlparse(request_url))
        headers['CE-Resource-Host'] = parts[1]
        parts[1] = SYSTEM_HOST
        return urlparse.urlunparse(tuple(parts))
    else:
        return request_url

def intra_system_get(request_url, headers={}):
    get_url = prepare_intra_system_call(request_url, headers)
    return requests.get(get_url, headers=headers)

CONTENT_RDF_JSON_HEADER = {
    'Content-type' : 'application/rdf+json+ce',
    'Cookie' : 'SSSESSIONID=%s' % cryptography.encode_jwt({'user': ADMIN_USER}),
    'ce-post-reason' : 'ce-create'
    }

def intra_system_post(request_url, data, headers=CONTENT_RDF_JSON_HEADER):
    post_url = prepare_intra_system_call(request_url, headers)
    return requests.post(post_url, headers=headers, data=json.dumps(data, cls=RDF_JSON_Encoder), verify=False)
    return None

