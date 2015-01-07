import logging

logger=logging.getLogger(__name__)

def create_document(user, document, public_hostname, tenant, namespace, resource_id=None):
    """
    Create a new document in the collection identified by 'public_hostname', 'tenant', and 'namespace'.

    The 'document' argument specifies the new document to be stored, in rdf_json format. Before it's put
    into the database, the rdf_json is first converted to the following storage format:

      {
        '_id': docId (may be provided by caller in '' subject in json_ld, or a value provided here)
        '_modificationCount': number
        '@id': document_url (with domain replaced by "urn:ce:" and periods escaped to %2E)
        '@graph': [
          {
            '@id': subject_url, (with domain replaced by "urn:ce:" if the url is on the same site and periods escaped to %2E)
            <predicate>: {'type': <rdf-json type>, 'value': <value>, 'datatype'=<datatype>}
            ... repeat ...
          },
          {
            ... repeat for additional subjects ...
          }
        ]
      }

    Return:
        Success: (201, <new-document-url:string>, <new-document:rdf_json>)
        Error: (<status-code:int>, None, <errror-msg:string>)
    """
    return 400, None, 'TODO'

def execute_query(user, query, public_hostname, tenant, namespace, projection=None):
    """
    Execute the specified 'query' against the collection identified by 'public_hostname', 'tenant',
    and 'namespace'.

    This fuction always succeeds and returns a list of 0 or more matching documents.

    Return:
        Success: (200, [<result-document1:rdf_json>, <result-document2:rdf_json>, ...])
        Error: no errors
    """
    return 400, 'TODO'

def get_document(user, public_hostname, tenant, namespace, documentId):
    """
    Get the document specified by 'public_hostname', 'tenant', 'namespace', and 'document_id'.

    Return:
        Success: (200, <result-document:rdf_json>)
        Error: (<status-code:int>, <errror-msg:string>)
    """
    return 400, 'TODO'

def delete_document(user, public_hostname, tenant, namespace, document_id):
    """
    Delete the document specified by 'public_hostname', 'tenant', 'namespace', and 'document_id'.

    If the document doesn't exist, this fuction is a NO-OP.

    Return:
        Success: (200, None)
        Error: no errors
    """
    return 400, 'TODO'

def patch_document(user, mod_count, new_values, public_hostname, tenant, namespace, document_id):
    """
    Patch the document specified by 'public_hostname', 'tenant', 'namespace', and 'document_id' with the
    content in 'document'.

    The patch 'document' must contain a list with 2 entries:

        [<modification-count:int>, <patch-document:rdf_json>]

    The modification count must be the value of the resource's modification count that was last read by the
    client in a GET of the resource. If the modification count in the database does not match the modification
    count provided by the client, the update query will fail and an HTTP 409 (Conflict) status code will be
    returned. If the update query succeeds, the modification count in the database will be incremented by 1,
    the updates will be made, and an HTTP 200 (OK) status code will be returned. A history document will also
    be created to capture the previous state of the resource.

    Note that creating a history document is idempotent and safe (in practice, if not in principle).
    This means that if there is a failure after creating the history document, and before the patch operation,
    the whole thing can be safely re-run. This may result in two identical history documents, where nomally
    there would be a difference between any two history documents, but this is perfectly harmless. Only the
    history document whose ID is referenced in the successful patch operation will ever be looked at, so the
    other is just wasting a little disk space.

    Return:
        Success: (200, None)
        Error: (<status-code:int>, <errror-msg:string>)
    """
    return 400, 'TODO'
    
def drop_collection(user, public_hostname, tenant, namespace):
    return # TODO

def get_prior_versions(user, public_hostname, tenant, namespace, history):
    return 400, 'TODO'

def tenant_names(namespace):
    return [] # TODO
