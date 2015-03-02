from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.errors import ConnectionFailure
from datetime import datetime
from dateutil import tz
from storage_mapping import rdf_json_from_storage
from storage_mapping import query_to_storage
from storage_mapping import storage_value_from_rdf_json
from storage_mapping import predicate_to_mongo
from storage_mapping import fix_up_url_for_storage
from base_constants import URL_POLICY as url_policy
import os
import threading
import logging
import time

logger=logging.getLogger(__name__)

def get_timestamp():
    #return datetime.utcnow()
    return datetime.now(tz.tzutc())

try:
    MONGO_CLIENT = MongoClient(os.environ['MONGODB_DB_HOST'], int(os.environ['MONGODB_DB_PORT']), tz_aware=True)
except ConnectionFailure, e:
    # On connection error sleep for 10 seconds and try again, Mongo might still be coming up.
    logger.info("Sleeping for 10 seconds hoping that MongoDB starts accepting connections")
    time.sleep(10)
    MONGO_CLIENT = MongoClient(os.environ['MONGODB_DB_HOST'], int(os.environ['MONGODB_DB_PORT']), tz_aware=True)
    
MONGODB_DB_NAME = os.environ['MONGODB_DB_NAME'] if 'MONGODB_DB_NAME' in os.environ else os.environ['APP_NAME']
MONGO_DB = MONGO_CLIENT[MONGODB_DB_NAME]
if 'MONGODB_DB_USERNAME' in os.environ:
    MONGO_DB.authenticate(os.environ['MONGODB_DB_USERNAME'], os.environ['MONGODB_DB_PASSWORD'])

next_id = 1
next_history_id = 1
lineage = None
history_lineage = None
inc_lock = threading.Lock()
def get_lineage():
    lineages_collection = MONGO_DB['lineages_collection']
    lineages_collection.ensure_index( 'lineage_value' )
    result = MONGO_DB.command(
        'findAndModify',
        'lineages_collection',
        query  = {'_id': 'lineage_document'},
        update = {'$inc': {'lineage_value': 1}},
        new    = True,
        upsert = True,
        full_response = True)
    if not result['ok']: # "No matching object found"
        logger.debug('find_and_modify_command failed to find or create lineage_document - errmsg: %s datetime: %s' % (result['errmsg'],  datetime.now()))
    else:
        lastErrorObject = result['lastErrorObject']
        if lastErrorObject['n'] == 1:
            lineage = result['value']['lineage_value']
            if lastErrorObject['updatedExisting']:
                logger.debug('find_and_modify_command successfully incremented lineage property of existing document. New value is: %d proc_id: %s datetime: %s' % (lineage, os.getpid(),  datetime.now()))
            else:
                logger.debug('find_and_modify_command successfully created new lineage document. Value of lineage property is: %d proc_id: %s datetime: %s' % (lineage, os.getpid(),  datetime.now()))
            return lineage
        else:
            if lastErrorObject['updatedExisting']:
                logger.debug('find_and_modify_command failed to increment lineage property of existing document. Proc_id: %s datetime: %s' % (os.getpid(), datetime.now()))
            else:
                logger.debug('find_and_modify_command failed to create initial lineage document Proc_id: %s datetime: ' % (os.getpid(),  datetime.now()))
    return -1

#TODO: The following constants are also defined in storage_mapping. Can't we put them in one place and share?
DC = 'http://purl.org/dc/terms/'
CE = 'http://ibm.com/ce/ns#'
XSD = 'http://www.w3.org/2001/XMLSchema#'
CREATOR = DC+'creator'
CREATED = DC+'created'
MODIFICATIONCOUNT = CE+'modificationCount'
LASTMODIFIED = CE+'lastModified'
LASTMODIFIEDBY = CE+'lastModifiedBy'
HISTORY = CE+'history'
ID = CE+'id'

SYSTEM_PROPERTIES = (CREATOR, CREATED, MODIFICATIONCOUNT, LASTMODIFIED, LASTMODIFIEDBY, HISTORY, '@id', '_id')

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
    if resource_id == None:
        resource_id = make_objectid()
    document_url = url_policy.construct_url(public_hostname, tenant, namespace, resource_id)
    subject_array = make_subject_array(document, public_hostname, document_url)
    if subject_array is None:
        return 400, None, 'cannot set system property'
    timestamp = get_timestamp()
    json_ld = {'_id' : resource_id, '@graph': subject_array, '@id' : fix_up_url_for_storage('', public_hostname, document_url)}
    json_ld['_modificationCount'] =  0
    json_ld['_created'] = json_ld['_lastModified'] = timestamp
    json_ld['_createdBy'] = json_ld['_lastModifiedBy'] = fix_up_url_for_storage(user, public_hostname, document_url)
    try:
        MONGO_DB[make_collection_name(tenant, namespace)].insert(json_ld)
    except DuplicateKeyError:
        return 409, None, 'duplicate document id: %s' % resource_id
    return 201, document_url, rdf_json_from_storage(json_ld, public_hostname) # status_code, headers, body (which could contain error info)

def execute_query(user, query, public_hostname, tenant, namespace, projection=None):
    """
    Execute the specified 'query' against the collection identified by 'public_hostname', 'tenant',
    and 'namespace'.

    This fuction always succeeds and returns a list of 0 or more matching documents.

    Return:
        Success: (200, [<result-document1:rdf_json>, <result-document2:rdf_json>, ...])
        Error: no errors
    """
    collection_url = url_policy.construct_url(public_hostname, tenant, namespace, None)
    query = query_to_storage(query, public_hostname, collection_url)
    logger.debug('execute_query: MongoDB query %s', query)
    if projection is None:
        cursor = MONGO_DB[make_collection_name(tenant, namespace)].find(query)
    else:
        # Note: projection must NOT suppress the @id field (@id is needed by the storage format conversion routine)
        cursor = MONGO_DB[make_collection_name(tenant, namespace)].find(query, projection)
    result = get_query_result(cursor, public_hostname)
    #logger.debug('execute_query: MongoDB result %s', result)
    return 200, result

def get_document(user, public_hostname, tenant, namespace, documentId):
    """
    Get the document specified by 'public_hostname', 'tenant', 'namespace', and 'document_id'.

    Return:
        Success: (200, <result-document:rdf_json>)
        Error: (<status-code:int>, <errror-msg:string>)
    """
    cursor = MONGO_DB[make_collection_name(tenant, namespace)].find({'_id': documentId})
    try: document = cursor.next()
    except StopIteration: document = None
    if document is not None:
        document = rdf_json_from_storage(document, public_hostname)
        return 200, document
    else:
        return 404, '404 not found'

def delete_document(user, public_hostname, tenant, namespace, document_id):
    """
    Delete the document specified by 'public_hostname', 'tenant', 'namespace', and 'document_id'.

    If the document doesn't exist, this fuction is a NO-OP.

    Return:
        Success: (200, None)
        Error: no errors
    """
    MONGO_DB[make_collection_name(tenant, namespace)].remove(document_id, True)
    #TODO: check how many things Mongo actually deleted...
    return 200, None

def drop_collection(user, public_hostname, tenant, namespace):
    MONGO_DB[make_collection_name(tenant, namespace)].drop()

def create_history_document(user, public_hostname, tenant, namespace, document_id):
    cursor = MONGO_DB[make_collection_name(tenant, namespace)].find({'_id': document_id})
    try: storage_json = cursor.next()
    except StopIteration: storage_json = None
    if storage_json is not None:
        storage_json['_versionOfId'] = storage_json['_id']
        storage_json['_versionOf'] = storage_json['@id']
        history_objectId = make_historyid()
        storage_json['_id'] = history_objectId
        history_collection_name = make_collection_name(tenant, namespace + '_history')
        history_document_url = url_policy.construct_url(public_hostname, tenant, namespace + '_history', history_objectId)
        storage_json['@id'] = fix_up_url_for_storage('', public_hostname, history_document_url)
        MONGO_DB[history_collection_name].insert(storage_json)
        return 201, history_document_url
    else:
        return 404, None

def get_prior_versions(user, public_hostname, tenant, namespace, history):
    query = {'@id': {'$in': [fix_up_url_for_storage(version, public_hostname, '/') for version in history]}}
    cursor = MONGO_DB[make_collection_name(tenant, namespace + '_history')].find(query)
    result = get_query_result(cursor, public_hostname)
    #logger.debug(result)
    return 200, result

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
    try:
        mod_count = int(mod_count)
    except ValueError:
        return 400, 'modification count must be an integer: %s' % mod_count

    status, history_document_id = create_history_document(user, public_hostname, tenant, namespace, document_id)
    if status == 201:
        if mod_count == -1:
            mod_count_criteria = False
        else:
            mod_count_criteria = True
        document_url = url_policy.construct_url(public_hostname, tenant, namespace, document_id)
        delete_subject_urls = [ fix_up_url_for_storage(x, public_hostname, document_url) for x in new_values.iterkeys() if new_values[x] is None]
        collection_name = make_collection_name(tenant, namespace)
        if len(delete_subject_urls) != 0:
            criteria = {'_id' : document_id}
            patch = {'$inc' : {'_modificationCount' : 1}, '$pull': { '@graph': { '@id': { '$in': delete_subject_urls } } }, '$push': {'_history' : history_document_id} }
            last_err = MONGO_DB[collection_name].update(criteria, patch)
            if last_err['n'] == 1:
                mod_count = mod_count + 1
            else:
                return 409, 'unexpected update count %s' % last_err
        for subject_url, subject_node in new_values.iteritems(): # have to patch one subject at a time, unfortunately
            if subject_node is None: continue
            # first assume the subject is already in the @graph array, and construct a query that will modify it
            criteria = {subject_url : {}}
            criteria = query_to_storage(criteria, public_hostname, document_url)
            criteria['_id'] = document_id
            if mod_count_criteria:
                criteria['_modificationCount'] = mod_count
            subject_sets = {'_lastModified' : get_timestamp(), '_lastModifiedBy': user}
            subject_unsets = {}
            for predicate, value_array in subject_node.iteritems():
                if predicate in SYSTEM_PROPERTIES or predicate == '_id': return 400, 'cannot set system property'
                if isinstance(value_array, (list, tuple)):
                    if len(value_array) > 0:
                        subject_sets['@graph.$.' + predicate_to_mongo(predicate)] = [storage_value_from_rdf_json(value, public_hostname, document_url) for value in value_array]
                    else:
                        subject_unsets['@graph.$.' + predicate_to_mongo(predicate)] = 1
                elif value_array == None:
                    subject_unsets['@graph.$.' + predicate_to_mongo(predicate)] = 1
                else:
                    subject_sets['@graph.$.' + predicate_to_mongo(predicate)] = storage_value_from_rdf_json(value_array, public_hostname, document_url)
            patch = {'$inc' : {'_modificationCount' : 1}, '$set' : subject_sets, '$push': {'_history' : history_document_id}}

            # mongo has breaking change with version 2.5.x and does not allow $unset to be empty. Check subject_unsets before inserting into patch
            if len(subject_unsets):
                patch['$unset'] = subject_unsets

            last_err = MONGO_DB[collection_name].update(criteria, patch)
            if last_err['n'] == 1:
                mod_count = mod_count + 1
            else:
                # our assumption that the subject is already in the @graph array must have been wrong. Construct a query that will add the subject
                criteria = {'_id': document_id}
                if mod_count_criteria:
                    criteria['_modificationCount'] = mod_count
                subject_sets = {'_lastModified' :get_timestamp(), '_lastModifiedBy': user}
                new_subject = {'@id': fix_up_url_for_storage(subject_url, public_hostname, document_url)}
                for predicate, value_array in subject_node.iteritems():
                    if predicate in SYSTEM_PROPERTIES or predicate == '_id': return 400, 'cannot set system property'
                    if isinstance(value_array, (list, tuple)):
                        if len(value_array) > 0:
                            new_subject[predicate_to_mongo(predicate)] = [storage_value_from_rdf_json(value, public_hostname, document_url) for value in value_array]
                    else:
                        new_subject[predicate_to_mongo(predicate)] = storage_value_from_rdf_json(value_array, public_hostname, document_url)
                patch = {'$inc' : {'_modificationCount' : 1}, '$set' : subject_sets, '$push': {'_history' : history_document_id, '@graph': new_subject}}
                last_err = MONGO_DB[collection_name].update(criteria, patch)
                if last_err['n'] == 1:
                    mod_count = mod_count + 1
                else:
                    return 409, 'unexpected update count %s' % last_err
        return 200, None
    else:
        return status, 'failed to create history document'

def make_objectid():
    global next_id
    global lineage
    with inc_lock:
        if not lineage:
            lineage = str(get_lineage())
        rslt = next_id
        next_id += 1
    return '.'.join((lineage, str(rslt)))

def make_historyid():
    global next_history_id
    global history_lineage
    with inc_lock:
        if not history_lineage:
            history_lineage = str(get_lineage())
        rslt = next_history_id
        next_history_id += 1
    return '.'.join((history_lineage, str(rslt)))

def get_query_result(cursor, public_hostname):
    batchSize = 100
    cursor.batch_size(batchSize)
    response = []
    for _ in range(batchSize): # TODO: how can client GET subsequent batches, if there are more?
        try: document = cursor.next()
        except StopIteration: break
        document = rdf_json_from_storage(document, public_hostname)
        response.append(document)
    return response

def make_subject_array(rdf_json, public_hostname, path_url):
    subject_array = []
    for subject, subject_node in rdf_json.iteritems():
        json_ld_subject_node = {}
        for predicate, value_array in subject_node.iteritems():
            if subject == rdf_json.graph_url and predicate in SYSTEM_PROPERTIES:
                return None
            predicate = predicate_to_mongo(predicate)
            value = [storage_value_from_rdf_json(item, public_hostname, path_url) for item in value_array] if isinstance(value_array, (list, tuple)) else storage_value_from_rdf_json(value_array, public_hostname, path_url)
            json_ld_subject_node[predicate] = value
        json_ld_subject_node['@id'] = fix_up_url_for_storage(subject, public_hostname, path_url)
        subject_array.append(json_ld_subject_node)
    return subject_array

def make_collection_name(tenant, namespace):
    return tenant + '/' + namespace

def tenant_names(namespace):
    collection_names = MONGO_DB.collection_names()
    return [name_split[0] for name_split in [collection_name.split('/') for collection_name in collection_names] if len(name_split) > 1 and name_split[1] == namespace]
