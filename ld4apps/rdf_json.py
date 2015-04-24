import json
import datetime, numbers
from UserDict import UserDict
from dateutil.parser import parse as to_datetime
from base_constants import XSD, LDP
import urlparse

_invalid_uri_chars = '<>" {}|\\^`'

def _is_valid_uri(uri):
    for c in _invalid_uri_chars: 
        if c in uri: return False
    return True

class URI(object):
    def __init__(self, uri_string):
        if isinstance(uri_string, URI):
            uri_string = str(uri_string)
        else:
            if not isinstance(uri_string, basestring) or not _is_valid_uri(uri_string): raise ValueError(repr(uri_string))
        self.uri_string = uri_string   
        
    def __eq__(self, other):
        return isinstance(other, URI) and self.uri_string == other.uri_string

    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __hash__(self):
        return self.uri_string.__hash__()
        
    def __repr__(self):
        return 'URI(%s)' % self.uri_string
        
    def __str__(self):
        return self.uri_string

class BNode(object):
    def __init__(self, bnode_string):
        self.bnode_string = bnode_string   
        
    def __eq__(self, other):
        return isinstance(other, BNode) and self.bnode_string == other.bnode_string

    def __ne__(self, other):
        return not self.__eq__(other)
        
    def __hash__(self):
        return self.bnode_string.__hash__()
        
    def __repr__(self):
        return 'BNode(%s)' % self.bnode_string
        
    def __str__(self):
        return self.bnode_string
        
class RDF_JSON_Document(UserDict):
    """
    This class is designed to avoid mapping between RDF-JSON and objects.
    The data is kept in its RDF-JSON format as it will be exchanged with the server.
    The purpose of the class is to wrap the RDF-JSON to provide accessor methods to make it easy to work with the RDF-JSON.
    """
    def __init__(self, aSource, graph_url=None, default_subject_url=None):
        if hasattr(aSource, 'status_code'):
            if aSource.status_code == 201 or aSource.status_code == 200:
                try:
                    self.data = json.loads(aSource.text, object_hook=rdf_json_decoder)
                except ValueError:
                    raise ValueError("No JSON object could be decoded from: %s" % aSource.text)
            if aSource.status_code == 201 :
                self.graph_url = aSource.headers['Location']
            elif aSource.status_code == 200:
                self.graph_url = aSource.headers['Content-Location']
            else: 
                self.graph_url = None
            self.default_subject_url = default_subject_url
        else:
            self.graph_url = graph_url
            self.default_subject_url = default_subject_url
            if isinstance(aSource, basestring): 
                self.data = json.loads(aSource, object_hook=rdf_json_decoder)
            elif hasattr(aSource, 'keys') :
                self.data = aSource
            else:
                raise ValueError('invalid source %s' % aSource)
    
    def graph_subject_node(self):
        if self.graph_url in self.data:
            return self.data[self.graph_url]
        else:
            return None
        
    def default_subject(self):
        if self.default_subject_url != None:
            return self.default_subject_url
        else:
            return self.graph_url

    def get_property(self, attribute, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            return self.data[subject_url_string][attribute]
        except (KeyError, IndexError):
            return None

    def get_properties(self, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        try:
            return self.data[subject_url_string]
        except (KeyError, IndexError):
            return None

    def set_properties(self, properties, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        if subject_url_string in self.data:
            self.data[subject_url_string].update(properties)
        else:
            self.data[subject_url_string] = properties.copy()
            
    def get_value(self, attribute, subject=None, default=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            rdf_value = self.data[subject_url_string][attribute]
        except (KeyError, IndexError):
            return default
        if isinstance(rdf_value, (list, tuple)):
            return rdf_value if len(rdf_value) > 0 else default
        else:
            return rdf_value
            
    def get_values(self, attribute, subject=None, default=None):
        if not subject:
            subject_url_string = self.default_subject()
        else:
            subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            result = self.data[subject_url_string][attribute]    
        except (KeyError, IndexError):
            return default if default != None else list()
        if isinstance(result, (list, tuple)):
            if len(result) > 0:
                return result
            else:
                return default if default != None else list() 
        else:
            return [result]
            
    def pop(self, attribute, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        return self.data[subject_url_string].pop(attribute)
    
    def get_subject(self, attribute, obj, default=None):
        object_uri_string = str(obj)
        attribute = str(attribute)
        for subject_uri_string, predicates in self.data.iteritems():
            if attribute in predicates:
                values = self.get_values(attribute, subject_uri_string)
                for value in values:
                    if isinstance(value, URI) and str(value) == object_uri_string:
                        return URI(subject_uri_string)
        return default

    def get_subjects(self, attribute, obj):
        object_uri_string = str(obj)
        attribute = str(attribute)
        subjects = []
        for subject_uri_string, predicates in self.data.iteritems():
            if attribute in predicates:
                values = self.get_values(attribute, subject_uri_string)
                for value in values:
                    if isinstance(value, URI) and str(value) == object_uri_string:
                        subjects.append(URI(subject_uri_string))
        return subjects
        
    def set_value(self, attribute, value, subject=None):
        if subject:
            subject_url_string = str(subject)
        else:
            subject_url_string = self.default_subject()
        attribute = str(attribute)
        if subject_url_string in self.data:
            if value is None:
                self.data[subject_url_string].pop(attribute, None)
            else:
                self.data[subject_url_string][attribute] = value
        else:
            self.data[subject_url_string] = {attribute: value}
            
    def add_triples(self, subject, predicates, value_array=None):
        subject_url_string = str(subject)
        if subject_url_string in self.data:
            decl = self[subject_url_string]
        else:
            decl = {}
            self[subject_url_string] = decl
        if hasattr(predicates, 'keys'): # it's a dict of predicates
            if value_array: 
                raise ValueError
            for predicate, new_value_array in predicates.iteritems():
                self.add_triples(subject_url_string, predicate, new_value_array)
            return
        else: # it must be a single predicate
            predicate_url_string = str(predicates)
            if predicate_url_string in decl:
                existing_values = decl[predicate_url_string]
                existing_value_array = existing_values if isinstance(existing_values, (list, tuple)) else [existing_values]
                value_array = value_array if isinstance(value_array, (list, tuple)) else [value_array]
                for value in value_array: 
                    if not value in existing_value_array:
                        decl[predicate_url_string] = existing_value_array # in case original was single-valued
                        existing_value_array.append(value)
            else:
                decl[predicate_url_string] = value_array
                
    def get_container_members(self):
        membershipResource = self.get_value(LDP+'membershipResource')
        hasMemberRelation = self.get_value(LDP+'hasMemberRelation')
        isMemberOfRelation = self.get_value(LDP+'isMemberOfRelation')
        if hasMemberRelation:
            result = self.get_values(hasMemberRelation, membershipResource)
        else:
            result = self.get_subjects(isMemberOfRelation, membershipResource)
        return result
            
    add_triple = add_triples
        
    def __repr__(self):
        return 'RDF_JSON_Document(%s, %s, %s)' %(self.graph_url, self.default_subject_url, json.dumps(self, indent=4, cls=RDF_JSON_Encoder))

    def __str__(self):
        return json.dumps(self, cls=RDF_JSON_Encoder, indent=4)

    def check_value(self, predicate, field_errors, value_type=None, required=True, subject=None, expected_value=None):
        value = self.get_property(predicate, subject)
        if value == None:
            if required:
                field_errors.append((predicate, 'must provide value'))
            return False
        if value_type and not isinstance(value, value_type):
            field_errors.append((predicate, '%s must be a %s, type is: %s' % (value, str(value_type), type(value)))) 
            return False
        if expected_value and not value == expected_value:
            field_errors.append((predicate, '%s must be equal to %s value is %s' % (value, expected_value, value))) 
            return False
        return True
    
    def getValue(self, attribute, default=None, subject=None):
        print "Obsolete function RDF_JSON_Document.getValue() - use get_value() instead."
        #import traceback
        #traceback.print_stack()
        return self.get_value(attribute, subject, default)

    def getValues(self, attribute, default=[], subject=None):
        print "Obsolete function RDF_JSON_Document.getValues() - use get_values() instead."
        return self.get_values(attribute, subject, default)

    def getSubject(self, attribute, default, obj):
        print "Obsolete function RDF_JSON_Document.getSubject() - use get_subject() instead."
        return self.get_subject(attribute, obj, default)

    def getSubjects(self, attribute, obj):
        print "Obsolete function RDF_JSON_Document.getSubjects() - use get_subjects() instead."
        return self.get_subjects(attribute, obj)

    def setValue(self, attribute, value, subject=None):
        print "Obsolete function RDF_JSON_Document.setValue() - use set_value() instead."
        return self.set_value(attribute, value, subject)
        
    def with_relative_references(self, graph_url=None):
        result = {}
        doc_url = graph_url if graph_url is not None else self.graph_url
        def storage_value(item):
            if isinstance(item, URI):
                return URI(urlunjoin(doc_url, str(item)))
            else:
                return item
        for subject, predicates in self.iteritems():
            storage_subject = urlunjoin(doc_url, subject)
            storage_predicates = {}
            result[storage_subject] = storage_predicates
            for predicate, values in predicates.iteritems():
                value = [storage_value(item) for item in values] if isinstance(values, (list, tuple)) else storage_value(values)
                storage_predicates[predicate] = value
        storage_doc = RDF_JSON_Document(result, urlunjoin(doc_url, self.graph_url))
        return storage_doc

    def with_absolute_references(self, graph_url):
        graph_url = str(graph_url)
        url_parts = urlparse.urlparse(graph_url)
        hostname = url_parts.netloc
        def abs_url_str(url_str):
            if url_str.startswith('/'):
                return ''.join(('http://', hostname, url_str))
            else:
                return url_str
        def rdf_value(item):
            if isinstance(item, URI):
                str_url = str(item)
                if str_url.startswith('/'):
                    return URI(''.join(('http://', hostname, str_url)))
                else:
                    return item
            else:
                return item
        result = {}
        for subject, predicates in self.iteritems(): 
            result_predicates = {}
            for predicate, storage_value_array in predicates.iteritems():
                if isinstance(storage_value_array, (list, tuple)):
                    result_predicates[predicate] = [rdf_value(item) for item in storage_value_array]
                else:
                    result_predicates[predicate] = rdf_value (storage_value_array)
            result[abs_url_str(subject)] = result_predicates
        return RDF_JSON_Document(result, abs_url_str(self.graph_url))
        
    def update_doc(self, rdf_document, subject=None):
        if subject:
            subject = str(subject)
            properties = rdf_document.get_properties(subject)
            if properties:
                self.set_properties(properties, subject)
        else:
            for subject, properties in rdf_document.iteritems():
                self.set_properties(properties, subject)
                    
def urlunjoin(base_url, url):
    if url == None:
        return str(base_url)
    else:
        url = str(url)
    if base_url == None:
        return str(url)
    else:
        base_url = str(base_url)
    if url.startswith('_:'): # you might expect that '_' would be parsed as a scheme  by urlparse, but it isn't
        return url
    else:
        o = urlparse.urlparse(url)
        if (o.scheme == '' or o.scheme == 'http' or o.scheme == 'https'):
            if o.netloc == '': # http(s) relative url
                if len(o.path) > 0 and o.path[0] == '/':
                    return url
                else:
                    abs_url = urlparse.urljoin(base_url, url) #make it absolute first 
                    o = list(urlparse.urlparse(abs_url))
                    o[0] = o[1] = '' # blank out the scheme and the netloc
                    return urlparse.urlunparse(o)
            else:
                b = urlparse.urlparse(base_url)
                if o.netloc == b.netloc:
                    o = list(o)
                    o[0] = o[1] = '' # blank out the scheme and the netloc
                    return urlparse.urlunparse(o)
                else:
                    return url
        else:
            return url
        
class RDF_JSON_Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, RDF_JSON_Document):
            return o.data
        elif isinstance(o, datetime.datetime):
            return {'type': 'literal', 'datatype' : XSD+'dateTime', 'value' : o.isoformat()}
        elif isinstance(o, URI):
            return {'type':'uri', 'value': str(o)}
        elif isinstance(o, BNode):
            return {'type':'bnode', 'value': str(o)}
        #elif isinstance(o, isodate.duration.Duration):
        #    return isodate.duration_isoformat(o)
        #elif isinstance(o, datetime.timedelta):
        #    return isodate.duration_isoformat(o)
        return super(RDF_JSON_Encoder, self).default(o) 

def rdf_json_decoder(dct):
    if 'type' in dct and 'value' in dct:
        if dct['type'] == 'uri':
            result = URI(dct['value'])
        elif dct['type'] == 'BNode':
            result = BNode(dct['value'])
        elif dct['type'] == 'literal':
            if 'datatype' in dct:
                if dct['datatype'] == XSD+'dateTime':
                    result = to_datetime(dct['value'])
                elif dct['datatype'] == XSD+'boolean' or dct['datatype'] == XSD+'string' or dct['datatype'] == XSD+'integer' or dct['datatype'] == XSD+'double':
                    result = dct['value']
            else:
                result = dct['value']
        else:
            result = dct
    else:
        result = dct
    return result
        
def rdf_json_value_struct(type, value, datatype=None):
    if type not in ('literal', 'bnode', 'uri'):
        raise ValueError(type)
    if datatype:
        if type != 'literal':
            raise ValueError('datatype only valid with type="literal": type %s datatype %s' % (type, datatype))
        return {'type': type, 'value':value,'datatype': datatype}
    else:
        return {'type': type, 'value':value} 

def rdf_json_value(value):
    # return an rdf-json value array for a simple python value
    if isinstance(value, basestring):
        return rdf_json_value_struct('literal', value)
    elif value is True or value is False:
        return rdf_json_value_struct('literal', value, XSD+'boolean')
    elif isinstance(value, numbers.Number):
        if isinstance(value, numbers.Integral):
            return rdf_json_value_struct('literal', value, XSD+'integer')
        else:
            return rdf_json_value_struct('literal', value, XSD+'double')
    elif isinstance(value, datetime.datetime):
        return rdf_json_value_struct('literal', value.isoformat(), XSD+'dateTime')
    elif hasattr(value, 'days'):
        return rdf_json_value_struct('literal', value, XSD+'duration')
    elif isinstance(value, URI):
        return rdf_json_value_struct('uri', str(value))
    elif isinstance(value, BNode):
        return rdf_json_value_struct('bnode', str(value))
    else:
        raise ValueError(repr(value))
        
def normalize(aDict):
    for key, predicates in aDict.iteritems():
        for predicate, value_array in predicates.iteritems():
            if not isinstance(value_array, (list, tuple)):
                value_array = [value_array]
            predicates[predicate] = [(value if hasattr(value, 'keys') else rdf_json_value(value)) for value in value_array] 
    return aDict

class RDF_json_to_compact_json_converter():
            
    def __init__(self, namespace_mappings):
        self.namespace_mappings = namespace_mappings
        
    def compact_predicate(self, predicate):
        for namespace, prefix in self.namespace_mappings.iteritems():
            if predicate.startswith(namespace): 
                return prefix + '_' + predicate[len(namespace):]
        return predicate
      
    def compact_json_value(self, value_struct, document, stack, deferred):
        if hasattr(value_struct, 'keys') and 'value' in value_struct:
            value = value_struct['value']
            value_type = value_struct['type']
            if (value_type == 'uri' or value_type == 'bnode') and value in document and value not in stack:
                return self.compact_json_stub(value, document, stack, deferred)
            else:
                return value
        elif isinstance(value_struct, URI):
            url_string = str(value_struct)
            if url_string in document and url_string not in stack:
                return self.compact_json_stub(url_string, document, stack, deferred)
            else:
                return url_string
        elif isinstance(value_struct, BNode):
            bnode_string = str(value_struct)
            compact_json = {}
            for predicate, value_array in document[bnode_string].iteritems():
                self.add_compact_property(compact_json, predicate, value_array, document, stack, deferred)
            return compact_json
        elif hasattr(value_struct, 'bnoode_string') and value_struct.bnode_string in document and value_struct.bnode_string not in stack:
            return self.compact_json_stub(value_struct.bnode_string, document, stack, deferred)
        elif isinstance(value_struct, datetime.datetime):
            return value_struct.isoformat()
        else:
            return value_struct
            
    def compact_json_stub(self, subject, document, stack, deferred):
        stack.add(subject) # only include each subject once
        compact_json = { '_subject': subject }
        deferred.append(compact_json)
        return compact_json
        
    def add_compact_property(self, compact_json, predicate, value_array, document, stack, deferred):
        key = self.compact_predicate(predicate)
        if isinstance(value_array, (list, tuple)):
            compact_json[key] = []
            for value_struct in value_array:
                compact_json[key].append(self.compact_json_value(value_struct, document, stack, deferred))
        else:
            compact_json[key] = self.compact_json_value(value_array, document, stack, deferred)
    
    def fill_in_stub(self, compact_json, document, stack, deferred):
        subject = compact_json['_subject']
        document_subject = document[subject]
        if LDP+'contains' in document_subject:
            self.add_compact_property(compact_json, LDP+'contains', document_subject[LDP+'contains'], document, stack, deferred)
        for predicate, value_array in document[subject].iteritems():
            if predicate != LDP+'contains':
                self.add_compact_property(compact_json, predicate, value_array, document, stack, deferred)

    def convert_to_compact_json(self, document):
        stack = set()
        deferred = []
        result = self.compact_json_stub(document.default_subject(), document, stack, deferred)
        while deferred:
            object = deferred.pop(0)
            self.fill_in_stub(object, document, stack, deferred)
        return result

class Compact_json_to_rdf_json_converter():
            
    def __init__(self, namespace_mappings):
        self.namespace_mappings = namespace_mappings
        self.bnode_count = 0

    def get_bnode(self):
        self.bnode_count += 1
        return BNode("_:b%s" % self.bnode_count)
             
    def get_value_from_string(self, predicate, value):
        if value.startswith('http:') or value.startswith('https:'):
            return URI(value)
        elif value.startswith('mailto:'): #TODO: do we really want to do this? What about other schemes?
            return URI(value)
        #TODO: do we also want:  elif <value is a "date" format>: return to_datetime(value)
        #TODO: anything else?
        return value
        
    def expand_predicate(self, predicate):
        for namespace, prefix in self.namespace_mappings.iteritems():
            if predicate.startswith(prefix + '_'): 
                return namespace + predicate[len(prefix)+1:]
        return predicate
    
    def convert_value(self, value, predicate, rdf_jso):
        if hasattr(value, 'keys'): # value is a value with a type, or a whole nested object
            if '_subject' in value: # value is a whole nested object
                converted_value = URI(value['_subject'])
                self.get_rdf_jso_from_compact_jso(rdf_jso, value)
            elif 'type' in value and 'value' in value: # value is a value of form {"type": "aType", "value": "aValue"}
                if value['type'] == 'uri':
                    converted_value = URI(value['value'])
                elif value['type'] == 'literal' and 'datatype' in value and value['datatype'] == 'http://www.w3.org/2001/XMLSchema#dateTime':
                    converted_value = to_datetime(value['value'])
                else:
                    raise ValueError("bad value in application/json")
            else:
                # value is a blank node nested object
                converted_value = self.get_bnode()
                value = dict(value) # make a copy
                value['_subject'] = str(converted_value)
                self.get_rdf_jso_from_compact_jso(rdf_jso, value)
        elif isinstance(value, basestring):
            converted_value = self.get_value_from_string(predicate, value)
        elif isinstance(value, (list, tuple)):
            converted_value = [self.convert_value(item, predicate, rdf_jso) for item in value]
        else:
            converted_value = value
        return converted_value
    
    def get_rdf_jso_from_compact_jso(self, rdf_jso, application_jso):
        rdf_predicates = {}
        subject = ''
        for key, value in application_jso.iteritems():
            if key == '_subject':
                subject = value
            else:
                predicate = self.expand_predicate(key)
                rdf_predicates[predicate] = self.convert_value(value, predicate, rdf_jso)
        if subject in rdf_jso:
            rdf_jso[subject].update(rdf_predicates) #TODO: need to find and then merge array values - update will simply replace
        else:
            rdf_jso[subject] = rdf_predicates

    def convert_to_rdf_json(self, application_jso):
        rdf_jso = {}
        self.get_rdf_jso_from_compact_jso(rdf_jso, application_jso)
        return rdf_jso
