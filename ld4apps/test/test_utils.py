import requests, json, jwt
from ld4apps.rdf_json import RDF_JSON_Encoder, RDF_JSON_Document, rdf_json_decoder
from ld4apps.base_constants import ADMIN_USER
from ld4apps.base_constants import RDF

SHARED_SECRET = 'our little secret'

encoded_signature = jwt.encode({'user': ADMIN_USER}, SHARED_SECRET, 'HS256')

POST_HEADERS = {
    'Content-type': 'application/rdf+json+ce', 
    'Cookie': 'SSSESSIONID=%s' % encoded_signature, 
    'ce-post-reason': 'ce-create' 
    }

PUT_HEADERS = {
    'Content-type': 'application/rdf+json+ce', 
    'Cookie': 'SSSESSIONID=%s' % encoded_signature
    }
    
POST_ACTION_HEADERS = {
    'Content-type': 'application/rdf+json+ce', 
    'Cookie': 'SSSESSIONID=%s' % encoded_signature, 
    }
    
PATCH_HEADERS = {
    'Content-type': 'application/json', 
    'Cookie': 'SSSESSIONID=%s' % encoded_signature, 
    }

GET_HEADERS = {
    'Accept': 'application/rdf+json+ce', 
    'Cookie': 'SSSESSIONID=%s' % encoded_signature, 
    } 

DELETE_HEADERS = {
    'Cookie': 'SSSESSIONID=%s' % encoded_signature, 
    }     

def get(url, resource_host=None):
    if resource_host is not None:
        headers = {'CE-Resource-Host': resource_host}
        headers.update(GET_HEADERS)
    else:
        headers = GET_HEADERS
    r = requests.get(str(url), headers=headers, verify=False)
    if r.status_code != 200:
        print '######## FAILED TO GET url: %s status_code: %s response_text: %s ' % (url, r.status_code, r.text)
        return None    
    return RDF_JSON_Document(r)

def prim_post(url, body, headers):
    r = requests.post(str(url), headers=headers, data=json.dumps(body, cls=RDF_JSON_Encoder), verify=False)
    if (r.status_code - 200) / 100 != 0: # not in the 200's
            print '######## FAILED TO CREATE url: %s status: %s text: %s body: %s' %(url, r.status_code, r.text, body)
            return None
    try:
        resource_type = RDF_JSON_Document(r).get_value(RDF+'type')
        resource_type = str(resource_type).split('#')[1]
    except:
        resource_type = 'unknown type'
    if r.status_code == 201:
        print '######## POSTed %s: location: %s, status: %d' % (resource_type, r.headers['location'], r.status_code)
        return RDF_JSON_Document(json.loads(r.text, object_hook=rdf_json_decoder), r.headers['location'])
    else:
        print '######## POSTed %s to: %s status: %d' % (resource_type, url, r.status_code)
        return None if r.status_code == 200 else {}
       
def post(url, body, resource_host=None, headers={}):
    headers = headers.copy()
    headers.update(POST_HEADERS)
    if resource_host is not None:
        headers['CE-Resource-Host'] = resource_host
    return prim_post(url, body, headers)
           
def post_action(url, body, resource_host=None):
    if resource_host is not None:
        headers = {'CE-Resource-Host': resource_host}
        headers.update(POST_ACTION_HEADERS)
    else:
        headers = POST_ACTION_HEADERS
    return prim_post(url, body, POST_ACTION_HEADERS)

def patch(url, body, resource_host=None):
    if resource_host is not None:
        headers = {'CE-Resource-Host': resource_host}
        headers.update(PATCH_HEADERS)
    else:
        headers = PATCH_HEADERS
    r = requests.patch(str(url), headers=headers, data=json.dumps(body, cls=RDF_JSON_Encoder), verify=False)
    if r.status_code != 200:
        print '######## FAILED TO PATCH url: %s status: %s text: %s body: %s' %(url, r.status_code, r.text, body)
        return None
    try:
        patched_resource =  RDF_JSON_Document(r)
        resource_type = str(patched_resource.getValue(RDF+'type')).split('#')[1]
    except:
        patched_resource =  None
        resource_type = 'unknown type'
    print '######## PATCHed %s: %s, status: %d' % (resource_type, url, r.status_code)
    return patched_resource
    
def delete(url, resource_host=None):
    if resource_host is not None:
        headers = {'CE-Resource-Host': resource_host}
        headers.update(DELETE_HEADERS)
    else:
        headers = DELETE_HEADERS
    r = requests.delete(str(url), headers=headers)
    if r.status_code != 200 and r.status_code != 204:
        print '######## FAILED TO DELETE url: %s status: %s text: %s' %(url, r.status_code, r.text)
        return None
    print '######## DELETEed resource: %s, status: %d text: %s' % (url, r.status_code, r.text)

def put(url, body, resource_host=None):
    if resource_host is not None:
        headers = {'CE-Resource-Host': resource_host}
        headers.update(PUT_HEADERS)
    else:
        headers = PUT_HEADERS
    r = requests.put(str(url), headers=headers, data=json.dumps(body, cls=RDF_JSON_Encoder), verify=False)
    if r.status_code != 200 and r.status_code != 201:
        print '######## FAILED TO PUT url: %s status: %s text: %s' %(url, r.status_code, r.text)
        return None
    print '######## PUT resource: %s, status: %d' % (url, r.status_code)
    return RDF_JSON_Document(r)