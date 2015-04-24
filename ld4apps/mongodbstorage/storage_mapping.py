import urlparse
import datetime
from ld4apps.rdf_json import RDF_JSON_Document
from ld4apps.rdf_json import rdf_json_value_struct
from ld4apps.rdf_json import URI
from ld4apps.rdf_json import BNode
from dateutil.parser import parse as to_date
from ld4apps.base_constants import XSD, RDF, CE, DC

STORAGE_PREFIX = 'urn:ce:'

REVISION = CE+'revision'
LASTMODIFIED = CE+'lastModified'
LASTMODIFIEDBY = CE+'lastModifiedBy'

CREATOR = DC+'creator'
CREATED = DC+'created'
#TODO: Think about whether we should use our own CE namespace (instead of DC) for CREATOR and CREATED properties, so that
#      we don't ever interfere with (wipe out) user-defined values.

def storage_relative_url(tenant, relative_url):
    return STORAGE_PREFIX + relative_url

def fix_up_url_for_storage(url, public_hostname, path_url):
    public_http_prefix = 'http://%s'%public_hostname
    public_https_prefix = 'https://%s'%public_hostname
    if url.startswith(public_http_prefix) and len(url) > len(public_http_prefix) and url[len(public_http_prefix)] == '/':
        return storage_relative_url(None, url[len(public_http_prefix):]) #make it storage-relative
    elif url.startswith(public_https_prefix) and len(url) > len(public_https_prefix) and url[len(public_https_prefix)] == '/':
        return storage_relative_url(None, url[len(public_https_prefix):]) #make it storage-relative
    elif url.startswith('_:'): # you might expect that '_' would be parsed as a scheme  by urlparse, but it isn't
        return url
    else:
        o = urlparse.urlparse(url)
        if (o.scheme == '' or o.scheme == 'http' or o.scheme == 'https') and o.netloc == '': # http(s) relative url
            abs_url = urlparse.urljoin(path_url, url) #make it absolute first
            if abs_url.startswith(public_http_prefix):
                return storage_relative_url(None, abs_url[len(public_http_prefix):]) #make it storage-relative
            elif abs_url.startswith(public_https_prefix):
                return storage_relative_url(None, abs_url[len(public_https_prefix):]) #make it storage-relative
            else: #oops - the hostname in the path_url must be different from public_hostname
                raise ValueError('#oops - the hostname in the path_url must be different from public_hostname. path_url: %s  url: %s  public_hostname: %s' % (path_url, url, public_hostname))
        else: #must be an absolute http url on a different host or an url with a scheme other than http(s)
            return url

def storage_value_from_rdf_json(rdf_json, public_hostname, path_url):
    if hasattr(rdf_json, 'keys'):
        rdf_type = rdf_json['type']
        if rdf_type == 'literal':
            value = rdf_json['value']
            datatype = rdf_json.get('datatype')
            if datatype == XSD+'dateTime':
                if not isinstance(value, datetime.datetime):
                    value = to_date(value)
                return value
            elif datatype == XSD+'boolean' or datatype == XSD+'string' or datatype == XSD+'integer' or datatype == XSD+'double' or datatype == XSD+'float' or not datatype:
                return value
            return rdf_json_value_struct('literal', value, datatype)
        elif rdf_type == 'uri':
            value = rdf_json['value']
            if hasattr(value, 'keys'):
                if '$in' in value:
                    return rdf_json_value_struct('uri', {'$in': [fix_up_url_for_storage(x, public_hostname, path_url) for x in value['$in']]})
                else: 
                    raise ValueError('unhandled clause %s' % value)
            else:
                return rdf_json_value_struct('uri', fix_up_url_for_storage(rdf_json['value'], public_hostname, path_url))
        elif rdf_type == 'bnode':
            return rdf_json
        else:
            raise ValueError(rdf_type)
    else:
        if isinstance(rdf_json, URI):
            return {'type':'uri','value': fix_up_url_for_storage(rdf_json.uri_string, public_hostname, path_url)}
        elif isinstance(rdf_json, BNode):
            return {'type':'bnode','value':rdf_json.bnode_string} 
        else:
            return rdf_json # hopefully it's a string, a number or a boolean, otherwise it won't work    
            
def restore_URL_from_storage(url, public_hostname):
    if url.startswith(STORAGE_PREFIX):
        public_url_prefix = 'http://%s'%public_hostname
        return public_url_prefix + url[len(STORAGE_PREFIX):]
    else: #must be absolute
        return url

def uri_string_from_storage(url_string, public_hostname):
    if url_string.startswith(STORAGE_PREFIX):
        public_url_prefix = 'http://%s'%public_hostname
        result = URI(public_url_prefix + url_string[len(STORAGE_PREFIX):])
    else: #must be absolute
        result = URI(url_string)
    return result
        
def restore_predicate_from_storage(predicate):
    if '%2E' in predicate: #need to escape dots in predicates to keep mongodb happy
        predicate = predicate.replace('%2E', '.')
    return predicate
    
def rdf_json_value_from_storage (storage_json, public_hostname):
    result = None
    if hasattr(storage_json, 'keys'):
        rj_type = storage_json['type']
        if rj_type == 'uri':
            url_string = storage_json['value']
            result = uri_string_from_storage(url_string, public_hostname)
        elif rj_type == 'literal':
            result = storage_json
        else:
            result = BNode(storage_json['value'])
    else:
        result = storage_json
    return result
        
def rdf_json_from_storage (storage_json, public_hostname):
    # return rdf_json format for a single document
    rdf_json = {}
    if '@graph' in storage_json:
        for storage_subject_node in storage_json['@graph']: 
            rdf_subject = {}
            for predicate, storage_value_array in storage_subject_node.iteritems():
                if predicate == "@id": 
                    pass
                else:
                    predicate = restore_predicate_from_storage(predicate)
                    if isinstance(storage_value_array, (list, tuple)):
                        rdf_subject[predicate] = [rdf_json_value_from_storage (item, public_hostname) for item in storage_value_array]
                    else:
                        rdf_subject[predicate] = rdf_json_value_from_storage (storage_value_array, public_hostname)
            rdf_json[restore_URL_from_storage(storage_subject_node['@id'], public_hostname)] = rdf_subject
    if '_versionOf' in storage_json:
        graph_subject_url = restore_URL_from_storage(storage_json['_versionOf'], public_hostname)
        version_url = restore_URL_from_storage(storage_json['@id'], public_hostname)
        rdf_json[version_url] = {CE+'versionOf': rdf_json_value_struct('uri', graph_subject_url), RDF+'type': rdf_json_value_struct('uri', CE+'Version')}
        if graph_subject_url not in rdf_json:
            rdf_json[graph_subject_url] = {}
    else:
        graph_subject_url = restore_URL_from_storage(storage_json['@id'], public_hostname)
        version_url = None
        if graph_subject_url not in rdf_json:
            rdf_json[graph_subject_url] = {}
    if '_modificationCount' in storage_json:
        rdf_json[graph_subject_url][REVISION] = str(storage_json['_modificationCount'])
    if '_lastModified' in storage_json:
        rdf_json[graph_subject_url][LASTMODIFIED] = storage_json['_lastModified']
    if '_lastModifiedBy' in storage_json:
        rdf_json[graph_subject_url][LASTMODIFIEDBY] = uri_string_from_storage(storage_json['_lastModifiedBy'], public_hostname)
    if '_created' in storage_json:
        rdf_json[graph_subject_url][CREATED] = storage_json['_created']
    if '_createdBy' in storage_json:
        rdf_json[graph_subject_url][CREATOR] = uri_string_from_storage(storage_json['_createdBy'], public_hostname)
    if '_history' in storage_json: 
        history = storage_json['_history']
        rdf_json[graph_subject_url][CE+'history'] = [URI(version) for version in history]
    return RDF_JSON_Document(rdf_json, version_url or graph_subject_url)
    
def predicate_to_mongo(predicate):
    # This method does two things, which perhaps should be separated. The first is to escape '.' in predicate names sinc eMongoDB cannot
    # accept those. The second is to convert paths of the form a->b->c used in queries to the a.b.c form that Mongo knows. The second is done
    # here because we currently use the same json_structure_to_storage method for queries that we use for resource representations. It might be
    # better in the future to use a different path for queries, in which case this logic may move.
    if '.' in predicate: 
        predicate = predicate.replace('.', '%2E')
    if '->' in predicate:
        predicate = predicate.replace('->', '.')
    return predicate
   
def query_value_to_storage(value, public_hostname, path_url):
    return storage_value_from_rdf_json(value, public_hostname, path_url)

def query_predicate_to_storage(predicate, value_array, public_hostname, path_url):
    if predicate == '$or':
        op1 = value_array[0].popitem()
        op2 = value_array[1].popitem()
        predicate1 = op1[0]
        value_array1 = op1[1]
        match_predicates1 = {}
        match_predicates1[predicate_to_mongo(predicate1)] = query_predicate_to_storage(predicate1, value_array1, public_hostname, path_url)
        predicate2 = op2[0]
        value_array2 = op2[1]
        match_predicates2 = {}
        match_predicates2[predicate_to_mongo(predicate2)] = query_predicate_to_storage(predicate2, value_array2, public_hostname, path_url)
        return [match_predicates1, match_predicates2]
    else:
        if isinstance(value_array, basestring) and value_array.startswith('_any'):
            return {'$exists' : True}
        elif hasattr(value_array, 'keys'):
            if len(value_array) == 1 and '$in' in value_array:
                values = value_array['$in']
                return {'$in' : [query_value_to_storage(value, public_hostname, path_url) for value in values]}
            elif len(value_array) == 1 and '$exists' in value_array:
                value = value_array['$exists']
                return {'$exists' : value}
            else:
                raise ValueError('unhandled clause %s' % value_array)
        else:
            isArray = isinstance(value_array, (list, tuple))
            if isArray and len(value_array) > 1:
                return {'$all' : [query_value_to_storage(value, public_hostname, path_url) for value in value_array]}
            else:
                return query_value_to_storage(value_array[0] if isArray else value_array, public_hostname, path_url)
                    
def query_to_storage(json_query, public_hostname, path_url):
    if '$query' in json_query:
        mongo_query_part = query_to_storage(json_query['$query'], public_hostname, path_url)
        predicate, ascending = json_query['$orderby'].popitem()
        predicate = predicate_to_mongo(predicate)
        return {'$query': mongo_query_part, '$orderby': {predicate: ascending}}
    match_array = []
    for subject, subject_map in json_query.iteritems():
        if subject.startswith('_any'):
            match_predicates = {} # would it be more correct to put something like {'$where' : 'this[@id] == this["@graph.0.@id"]'} ??
        else:  
            match_predicates = {'@id' : fix_up_url_for_storage(subject, public_hostname, path_url)}
        for predicate, value_array in subject_map.iteritems():
            match_predicates[predicate_to_mongo(predicate)] = query_predicate_to_storage(predicate, value_array, public_hostname, path_url)
        match_array.append({'@graph': {'$elemMatch': match_predicates}})
    if len(match_array) > 1:
        mongo_query = {'$and' : match_array}
    elif len(match_array) == 1:
        mongo_query = match_array[0]
    else:
        mongo_query = {}
    return mongo_query