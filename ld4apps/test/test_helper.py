
import requests, json, jwt
from rdf_json import URI, BNode, RDF_JSON_Encoder, RDF_JSON_Document, rdf_json_decoder
from base_constants import RDF, DC, AC, AC_ALL, ADMIN_USER, CE, VCARD, FOAF, ANY_USER, AC_T, AC_R, AC_C, AC_D, AC_W, AC_X
import pprint
pp = pprint.PrettyPrinter(indent=4)

HS_HOSTNAME = 'hostingsite.localhost:3001'
account_container_url = 'http://%s/account' % HS_HOSTNAME
ac_container_url = 'http://%s/ac' % HS_HOSTNAME

USER1_URL = '%s/user1#owner' % account_container_url
USER2_URL = '%s/user2#owner' % account_container_url


def make_headers(verb='GET', username=None, revision=None):
    if verb not in ('GET', 'POST', 'PATCH', 'DELETE'):
        raise Exception('invalid header type')

    header = {}

    if username is not None:
        encode_salt = 'our little secret'
        encoded_signature = jwt.encode({'user': username}, encode_salt, 'HS256')
        header.update({'Cookie': 'SSSESSIONID=%s' % encoded_signature})
    if verb == 'GET':
        header.update({'Accept': 'application/rdf+json+ce'})
    elif verb == 'POST':
        header.update({'Content-type': 'application/rdf+json+ce'})
        #this is something about POST Action
        header.update({'ce-post-reason': 'ce-create'})
    elif verb == 'PATCH':
        header.update({'Content-type': 'application/rdf+json+ce'})
        header.update({'CE-Revision': revision})

    return header


def container_crud_test(container_url, post_body, patch_prop, patch_value, username=ADMIN_USER):

    # create
    r_doc = create(container_url, post_body, username=username)
    resource_url = r_doc.default_subject()

    # read
    read(resource_url, username=username)

    # update
    update(resource_url, patch_prop, patch_value, username=username)

    # delete
    delete(resource_url, username=username)
    #   verify that the document has been deleted
    read(resource_url, username=username, assert_code=404)


def resource_access_test(resource_url, username, patch_prop, patch_value, assert_code_read=200, assert_code_update=200, assert_code_delete=200):
    # read
    read(resource_url, username=username, assert_code=assert_code_read)

    # update
    update(resource_url, patch_prop=patch_prop, patch_value=patch_value, username=username,
           assert_code_update=assert_code_update, assert_code_read=assert_code_read)

    # delete
    delete(resource_url, username=username, assert_code=assert_code_delete)


def create(container_url, post_body, username=None, assert_code=201):
    # test post
    headers = make_headers('POST', username=username)
    r = requests.post(container_url, headers=headers, data=json.dumps(post_body, cls=RDF_JSON_Encoder), verify=False)
    assert r.status_code == assert_code
    r_doc = RDF_JSON_Document(r)
    return r_doc


def read(resource_url, username=None, assert_code=200):
    body = {}
    headers = make_headers('GET', username=username)
    r = requests.get(resource_url, headers=headers, data=json.dumps(body, cls=RDF_JSON_Encoder), verify=False)
    assert r.status_code == assert_code
    return RDF_JSON_Document(r)


def update(resource_url, patch_prop, patch_value, username=None, assert_code_update=200, assert_code_read=200):
    # to update we need to get the existing document to get the modification count
    r_doc = read(resource_url, username=username, assert_code=assert_code_read)

    modcount = 0

    # if we expect to be able to read, get modcount and verify we are actually changing something
    if assert_code_read == 200:
        revision = r_doc[r_doc.default_subject()][CE+'revision']
         # check that the patch property's value isn't already what we're going to change it to
        assert r_doc[r_doc.default_subject()][patch_prop] != patch_value

    # do update
    r = update_simple(resource_url, username, revision, patch_prop, patch_value, assert_code_update)

    # if we expect success, verify that patch property changed
    if assert_code_update == 200:
        r_doc = RDF_JSON_Document(r)
        assert r_doc[r_doc.default_subject()][patch_prop] == patch_value


def update_simple(resource_url, username, modcount, patch_prop, patch_value, assert_code=200):
    # declare patch document
    patch_body = {
        '': {
            patch_prop: patch_value
        }
    }

    # do patch
    headers = make_headers('PATCH', username=username, modification_count=modcount)
    r = requests.patch(resource_url, headers=headers, data=json.dumps(patch_body, cls=RDF_JSON_Encoder), verify=False)

    assert r.status_code == assert_code
    return r


def delete(resource_url, username=None, assert_code=200):
    # delete
    body = {}  # TODO: not sure this is necessary
    headers = make_headers('DELETE', username=username)
    r = requests.delete(resource_url, headers=headers, data=json.dumps(body, cls=RDF_JSON_Encoder), verify=False)
    assert r.status_code == assert_code