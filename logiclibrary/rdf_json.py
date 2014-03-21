import json
import datetime, numbers
from UserDict import UserDict
from dateutil.parser import parse as to_datetime

XSD = 'http://www.w3.org/2001/XMLSchema#'
BP = 'http://open-services.net/ns/basicProfile#'

_invalid_uri_chars = '<>" {}|\\^`'

def _is_valid_uri(uri):
    for c in _invalid_uri_chars: 
        if c in uri: return False
    return True

class URI():
    def __init__(self, uri_string):
        if not isinstance(uri_string, basestring) or not _is_valid_uri(uri_string): raise ValueError(uri_string)
        self.uri_string = uri_string   
        
    def __eq__(self, other):
        return isinstance(other, URI) and self.uri_string == other.uri_string

    def __ne__(self, other):
        return not self.eq(other)
        
    def __hash__(self):
        return self.uri_string.__hash__()
        
    def __repr__(self):
        return 'URI(%s)' % self.uri_string
        
    def __str__(self):
        return self.uri_string

class BNode():
    def __init__(self, bnode_string):
        self.bnode_string = bnode_string   
        
    def __eq__(self, other):
        return isinstance(other, BNode) and self.bnode_string == other.bnode_string

    def __ne__(self, other):
        return not self.eq(other)
        
    def __hash__(self):
        return self.bnode_string.__hash__()
        
    def __repr__(self):
        return 'BNode(%s)' % self.bnode_string
        
    def __str__(self):
        return self.bnode_string
        
class RDF_JSON_Document(UserDict):
    # This class is designed to avoid mapping between RDF-JSON and objects.
    # The data is kept in its RDF-JSON format as it will be exchanged with the server.
    # The purpose of the class is to wrap the RDF-JSON to provide accessor methods to make it easy to work with the RDF-JSON.
    
    def __init__(self, aDict, graph_url=None, default_subject_url=None):
        self.graph_url = graph_url
        self.default_subject_url = default_subject_url
        self.data = aDict            
    
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

    def getProperty(self, attribute, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            return self.data[subject_url_string][attribute][0]
        except (KeyError, IndexError):
            return None
        
    def getProperties(self, attribute, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            return self.data[subject_url_string][attribute]
        except (KeyError, IndexError):
            return None

    def getValue(self, attribute, default=None, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            rdf_value = self.data[subject_url_string][attribute]
        except (KeyError, IndexError):
            return default
        if isinstance(rdf_value, (list, tuple)):
            return rdf_value[0] if len(rdf_value) > 0 else default
        else:
            return rdf_value
            
    def pop(self, attribute, subject=None):
        if not subject:
            subject = self.default_subject()
        subject_url_string = str(subject)
        attribute = str(attribute)
        return self.data[subject_url_string].pop(attribute)
    
    def getSubject(self, attribute, default, object):
        object_uri_string = str(object)
        attribute = str(attribute)
        for subject_uri_string, predicates in self.data.iteritems():
            if attribute in predicates:
                values = self.getValues(attribute, [], subject_uri_string)
                for value in values:
                    if isinstance(value, URI) and str(value) == object_uri_string:
                        return URI(subject_uri_string)
        return default

    def getSubjects(self, attribute, object):
        object_uri_string = str(object)
        attribute = str(attribute)
        subjects = []
        for subject_uri_string, predicates in self.data.iteritems():
            if attribute in predicates:
                values = self.getValues(attribute, [], subject_uri_string)
                for value in values:
                    if isinstance(value, URI) and str(value) == object_uri_string:
                        subjects.append(URI(subject_uri_string))
        return subjects
        
    def setValue(self, attribute, value, subject=None):
        if not subject:
            subject_url_string = self.default_subject()
        else:
            subject_url_string = str(subject)
        attribute = str(attribute)
        if subject_url_string in self.data:
            self.data[subject_url_string][attribute] = value
        else:
            self.data[subject_url_string] = {attribute: value}
        
    def getValues(self, attribute, default=[], subject=None):
        if not subject:
            subject_url_string = self.default_subject()
        else:
            subject_url_string = str(subject)
        attribute = str(attribute)
        try:
            result = self.data[subject_url_string][attribute]    
        except (KeyError, IndexError):
            return default
        if isinstance(result, (list, tuple)):
            if len(result) > 0:
                return result
            else:
                return default
        else:
            return [result]
        
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
        membershipSubject = self.getValue(BP+'membershipSubject')
        membershipObject = self.getValue(BP+'membershipObject')
        membershipPredicate = self.getValue(BP+'membershipPredicate')
        if membershipSubject:
            result = self.getValues(membershipPredicate, [], membershipSubject)
        else:
            result = self.getSubjects(membershipPredicate, membershipObject)
        return result
            
    add_triple = add_triples
        
    def __repr__(self):
        return 'RDF_JSON_Document(%s, %s, %s)' %(self.graph_url, self.default_subject_url, json.dumps(self, indent=4, cls=RDF_JSON_Encoder))
        
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
      
    def compact_json_value(self, value_struct, document, stack):
        if hasattr(value_struct, 'keys') and 'value' in value_struct:
            value = value_struct['value']
            value_type = value_struct['type']
            if (value_type == 'uri' or value_type == 'bnode') and value in document and value not in stack:
                return self.compact_json_object(value, document, stack)
            else:
                return value
        elif isinstance(value_struct, URI):
            url_string = str(value_struct)
            if url_string in document and url_string not in stack:
                return self.compact_json_object(str(value_struct), document, stack)
            else:
                return value_struct
        elif hasattr(value_struct, 'bnoode_string') and value_struct.bnode_string in document and value_struct.bnode_string not in stack:
            return self.compact_json_object(value_struct.bnode_string, document, stack)
        else:
            return value_struct
            
    def compact_json_object(self, subject, document, stack):
        stack = stack + [subject] # make our own copy to avoid updating caller's stack. This maximizes duplication while still breaking cycles.
        compact_json = { '_subject': subject } # @id causes problems with some data binding frameworks
        for predicate, value_array in document[subject].iteritems():
            key = self.compact_predicate(predicate)
            if isinstance(value_array, (list, tuple)):
                compact_json[key] = []
                for value_struct in value_array:
                    compact_json[key].append(self.compact_json_value(value_struct, document, stack))
            else:
                compact_json[key] = self.compact_json_value(value_array, document, stack)
        return compact_json

    def convert_to_compact_json(self, document):
        compact_json = self.compact_json_object(document.default_subject(), document, [])
        return compact_json