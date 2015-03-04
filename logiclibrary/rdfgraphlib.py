# This module converts rdflib Graphs to and from RDF/JSON (https://dvcs.w3.org/hg/rdf/raw-file/default/rdf-json/index.html#) format.
#
from rdflib import Graph, URIRef, BNode, Literal
import json

XSD_BOOLEAN = 'http://www.w3.org/2001/XMLSchema#boolean'
XSD_INTEGER = 'http://www.w3.org/2001/XMLSchema#integer'
XSD_DECIMAL = 'http://www.w3.org/2001/XMLSchema#decimal'
XSD_DOUBLE = 'http://www.w3.org/2001/XMLSchema#double'

CONTENT_RDF_XML = 'application/rdf+xml'
CONTENT_TEXT_TURTLE = 'text/turtle'
CONTENT_APPLICATION_X_TURTLE = 'application/x-turtle'
CONTENT_LD_JSON = 'application/ld+json'

RDF_SERIALIZE_MAP = {CONTENT_RDF_XML : 'xml',
                     CONTENT_TEXT_TURTLE : 'turtle',
                     CONTENT_APPLICATION_X_TURTLE : 'turtle',
                     CONTENT_LD_JSON : 'json-ld'}

# Convert an rdflib Graph to an RDF/JSON structure
def graph_to_rdfjson(graph):
    rdfjson = {}
    for s in set(graph.subjects()):
        # only unreferenced.. TODO: not if more than one ref!
        if isinstance(s, URIRef) or not any(graph.subjects(None, s)):
            _subject_to_node(graph, s, rdfjson)
    return rdfjson

# Convert an RDF/JSON structure to an rdflib Graph
def rdfjson_to_graph(rdfjson):
    graph = Graph()
    for s, s_triples in rdfjson.iteritems():
        subject = BNode(s) if s.startswith('_:') else URIRef(s)
        for p, v_list in s_triples.iteritems():
            if p == '_id': continue #TODO: should _id be exposed using a proper predicate URI?
            predicate = URIRef(p)
            for v in v_list:
                if v['type'] == 'uri':
                    value = URIRef(v['value'])
                elif v['type'] == 'bnode':
                    value = BNode(v['value'])
                else: # v['type'] == 'literal'
                    if 'datatype' in v \
                        and v['datatype'] != XSD_BOOLEAN: # workaround for rdflib bug (serializes "True" instead of "true")?
                        value = Literal(v['value'], datatype=v['datatype'])
                    else:
                        value = Literal(v['value'])
                graph.add((subject, predicate, value))
    return graph

# Serialize a graph with the specified content_type.
# Supported content_type values are "application/json" (JSON-LD), "application/rdf+xml", "text/turtle", and "application/x-turtle".
def serialize_graph(graph, content_type, wfile=None):
    return graph.serialize(wfile, RDF_SERIALIZE_MAP[content_type])

def _subject_to_node(graph, s, nodes):
    current = {}
    p_objs = {}
    for p, o in graph.predicate_objects(s):
        objs = p_objs.setdefault(p, [])
        objs.append(o)
    for p, objs in p_objs.items():
        p_key, node = _key_and_node(graph, p, objs, nodes)
        current[p_key] = node
    if isinstance(s, URIRef):
        s_key = str(s)
        s_type = 'uri'
    else:
        s_key = '_:%s' % str(s)
        s_type = 'bnode'
    nodes[s_key] = current
    return { 'type': s_type, 'value': s_key }

def _key_and_node(graph, p, objs, nodes):
    many = not len(objs) == 1
    node = None
    if not many:
        node = [ _to_raw_value(graph, objs[0], nodes) ]
    else:
        node = [ _to_raw_value(graph, o, nodes) for o in objs ]
    return str(p), node

def _to_raw_value(graph, o, nodes):
    if isinstance(o, BNode):
        return _subject_to_node(graph, o, nodes)
    elif isinstance(o, URIRef):
        return { 'type': 'uri', 'value': str(o) }
    elif isinstance(o, Literal):
        v = o.encode('utf-8')
        if o.language:
            lang = str(o.language)
            return { 'type': 'literal', 'value': v, 'lang': lang }
        elif o.datatype:
            rdftype = str(o.datatype)
            if rdftype == XSD_INTEGER or rdftype == XSD_DECIMAL or rdftype == XSD_DOUBLE or rdftype == XSD_BOOLEAN:
                v = json.loads(v)
            return { 'type': 'literal', 'value': v, 'datatype': rdftype }
        else:
            return { 'type': 'literal', 'value': v }

if __name__ == "__main__":
    testrdf = \
    '''
    _:N7410e0ae24084e2b857abb01b006c2d3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    _:N7410e0ae24084e2b857abb01b006c2d3 <http://xmlns.com/foaf/0.1/nick> "guest" .
    <http://localhost:3010/faq/entries/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://jazz.net/ns/faq#Entry> .
    <http://localhost:3010/faq/entries/1> <http://purl.org/dc/terms/created> "2013-02-19T17:47:36.729000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
    <http://localhost:3010/faq/entries/1> <http://purl.org/dc/terms/creator> _:N7410e0ae24084e2b857abb01b006c2d3 .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#answer> "I think so." .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#question> "Is it working?" .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#intval> 99 .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#intval> 100 .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#decimalval> 99.9 .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#doubleval> 9.9E2 .
    <http://localhost:3010/faq/entries/1> <http://jazz.net/ns/faq#boolval> true .
    <http://localhost:3010/faq/entries/1> <http://purl.org/dc/terms/contributor> _:N7410e0ae24084e2b857abb01b006c2d3 .
    '''
    g = Graph().parse(data=testrdf, format='n3')
    print("Start Graph:")
    print(g.serialize(format='nt', indent=4))
    
    rdfjson = graph_to_rdfjson(g)
    print("RDF/JSON:")    
    print(json.dumps(rdfjson, indent=4))
    
    g = rdfjson_to_graph(rdfjson)
    print("Converted Graph:")
    print(g.serialize(format='nt', indent=4))
