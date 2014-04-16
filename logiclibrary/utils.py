import os
import Cookie, cryptography
import uuid
import requests, urlparse
import json
from base_constants import ADMIN_USER
from rdf_json import RDF_JSON_Encoder

SYSTEM_HOST = os.environ.get('SYSTEM_HOST') if 'SYSTEM_HOST' in os.environ else None

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

