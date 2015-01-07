# This module can be used to build and manage a Tracked Resource Set and its corresponding Change Log segments.
# You create a Tracked Resource Set using the TrackedResourceSetBuilder class like this:
#
#   builder = TrackedResourceSetBuilder(public_hostname, collection_name)
#
# The first argument is the public host name of the generated and tracked resource URIs. The second argument
# is the MongoDB collection being tracked. The specified collection may contain an existing TRS with an
# already populated changeLog to be continued (appended to) by the new instance.
# 
# Once you've created the instance, you then call the following method for each event that you want to
# put into the log. For example, this will add a MODIFICATION-type entry to the log with a sequence number
# equal to 100:
#
#   builder.addChangeEntry('http://someResourceUri', TRS['Modification'], 100)
#
# The following call can be used to add another entry using a generated sequence number (i.e., the next
# consecutive value, 101 in this case):
#
#   builder.addChangeEntry('http://someOtherResourceUri', TRS['Modification'])
#
# If the Change Log has more than a page worth of entries (the default page size is 100), then the first
# segment will point to a continuation segment, which can be retrieved the same way, only using its URI 
# (e.g., 'http://example.com/tracking/cl1').
#
# The following method can be called at any time to (re)compute the base portion of the Tracked Resource Set:
#
#   builder.computeBase();
#
# TODO Add automatic support for truncating (removing old segments) of overly long ChangeLogs.
#
import sys, os
import datetime
from rdflib.graph import Graph
from rdflib.namespace import Namespace
from rdflib.term import Literal
from rdflib.term import URIRef
from rdflib.term import BNode
from rdfgraphlib import graph_to_rdfjson, rdfjson_to_graph
from rdf_json import RDF_JSON_Document
from base_constants import URL_POLICY as url_policy

if __name__ == '__main__':
    os.environ['APP_NAME'] = 'LifecycleConcepts'
    os.environ['MONGODB_DB_HOST'] = 'localhost'
    os.environ['MONGODB_DB_PORT'] = '27017'

from storage import OPERATION_PRIMITIVES as operation_primitives

DEFAULT_SEGMENT_SIZE = 100

TRS_DOCUMENT_ID = 'trs'
CL_DOCUMENT_ID_PREFIX = 'cl-'
BASE_DOCUMENT_ID_PREFIX = 'base-'

PRIVILEGED_USER = 'http://ibm.com/user/Frank'

TRS = Namespace('http://jazz.net/ns/trs#')
RDF = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
RDFS = Namespace('http://www.w3.org/2000/01/rdf-schema#')

# Generate the RDF contents for the specified segment of the specified ChangeLog. 
# Segments are numbered 1 to N, N being the most recent. Segment number N will grow up to twice
# the specified segmentSize before being split into two segments. All other segments are exactly
# the specified size.
# 
# changes: the current ordered list of entries for the ChangeLog. The last entry in the list is the most recent change.
# isTrackedResourceSet: true => generate current segment as a blank node (to be inserted in a TrackedResourceSet).
# changeLogURIPrefix: the URI prefix of the ChangeLog segments (i.e., "<prefix>/1", "<prefix>/2", ...).
# segmentNo: the segment number to generate (must be > truncatedSegmentNo).
# truncatedSegmentNo: the segment number of the continuation of the log represented by "changes" (0 => none).
# segmentSize: the segment size.
# outputGraph: the Graph in which to generate the ChangeLog RDF.
# 
# return: the generated segment resource.
def generateSegment(changes, isTrackedResourceSet, changeLogURIPrefix, segmentNo, truncatedSegmentNo, segmentSize, outputGraph):
    currentSegment = truncatedSegmentNo + len(changes) / segmentSize
    if currentSegment == 0:
        currentSegment += 1
    
    assert(segmentNo > truncatedSegmentNo and segmentNo <= currentSegment)
    
    if segmentNo == currentSegment:
        if isTrackedResourceSet:
            segmentResource = BNode() # resource will be a blank node
        else: 
            segmentResource = URIRef(changeLogURIPrefix)
    else:
        segmentResource = URIRef(changeLogURIPrefix + str(segmentNo))
    
    nextSegmentNo = segmentNo - 1
    if nextSegmentNo != 0:
        nextPageResource = changeLogURIPrefix + str(nextSegmentNo)
        outputGraph.add((segmentResource, TRS['previous'], Literal(nextPageResource)))

    startIndex = (segmentNo - truncatedSegmentNo - 1) * segmentSize
    if startIndex + (segmentSize * 2) <= len(changes):
        endIndex = startIndex + segmentSize
    else:
        endIndex = len(changes)
    
    changesList = RDF['nil']
    for index in range(startIndex, endIndex):
        change = changes[index]
        changeResource = URIRef(change.identifier)
        outputGraph.add((changeResource, TRS['changed'], URIRef(change.changed)))
        outputGraph.add((changeResource, RDF['type'], URIRef(change.kind)))
        outputGraph.add((changeResource, TRS['order'], Literal(change.order)))

        listEntry = BNode() # anonymous
        outputGraph.add((listEntry, RDF['first'], changeResource))
        outputGraph.add((listEntry, RDF['rest'], changesList))
        changesList = listEntry

    outputGraph.add((segmentResource, TRS['changes'], changesList))
    outputGraph.add((segmentResource, RDF['type'], TRS['ChangeLog']))
    
    return segmentResource

def generateIdentifier(order):
    return "urn:trs:" + timestamp() + ":" + str(order);

def timestamp():
    return datetime.datetime.utcnow().isoformat()

# TODO: replace this method with a better implementation that knows how to handle different cases: 
#       1. docid is always url_parts[-1]
#       2. host is always url_parts[2]
#       3. collection is either 3/4 or just 4???
#   Alternative is have trsbuilder store backpointer to its logic_tier client and call a method in logic_tier
def host_collection_and_document_names_from_url(url):
    # URLs are of the form http://hostname/collection/docid, but LC collection is of the form siteid/name (Site is site/name)
    url_parts = url.split('/')
    if url_parts[3] == 'site': # Kludge -- is this right????
        return (url_parts[2], url_parts[4], url_parts[5])
    else:
        return (url_parts[2], '%s/%s' % (url_parts[3], url_parts[4]), url_parts[5])

def getGraph(graphURI):
    host_name, collection_name, document_id = host_collection_and_document_names_from_url(graphURI)
    status, rdfjson = operation_primitives.get_document(PRIVILEGED_USER, host_name, collection_name, document_id)
    if status == 200:
        graph = rdfjson_to_graph(rdfjson)
        return graph
    else:
        return None

def storeGraph(graphURI, graph):
    rdfjson = graph_to_rdfjson(graph)
    host_name, collection_name, document_id = host_collection_and_document_names_from_url(graphURI)
    document = RDF_JSON_Document(rdfjson, graphURI)
    #TODO: This implementation relies on replace_document using upsert=true. Efficient, but result is missing created/creator properties.
    operation_primitives.replace_document(PRIVILEGED_USER, document, host_name, collection_name, document_id)
    
def deleteGraph(graphURI):
    host_name, collection_name, document_id = host_collection_and_document_names_from_url(graphURI)
    operation_primitives.delete_document(PRIVILEGED_USER, host_name, collection_name, document_id)
    
class ChangeEntry:
    def __init__(self, kind, changed, order, identifier):
        self.kind = kind
        self.changed = changed
        self.order = order
        self.identifier = identifier
    
class TrackedResourceSetBuilder:
    def __init__(self, public_hostname, collection_name, segmentSize=DEFAULT_SEGMENT_SIZE, computeBase=True):
        self.publicHostname = public_hostname
        self.collectionName = collection_name
        self.trackedResourceSetURIBase = 'http://%s/%s_tracking/' % (public_hostname, collection_name)
        self.trackedResourceSetURI = self.trackedResourceSetURIBase + TRS_DOCUMENT_ID
        self.changeLogURIBase = self.trackedResourceSetURIBase + CL_DOCUMENT_ID_PREFIX
        self.segmentSize = segmentSize
        self.segmentNo = 1
        self.currentChanges = []
        
        trsGraph = getGraph(self.trackedResourceSetURI)
        if trsGraph is not None: # Already have an existing TrackedResourceSet/ChangeLog?
            self.currentBaseURI = trsGraph.objects(URIRef(self.trackedResourceSetURI), TRS['base']).next()
            changeLogURI = trsGraph.objects(URIRef(self.trackedResourceSetURI), TRS['changeLog']).next()
            self.initCurrentChanges(trsGraph, changeLogURI)
            if computeBase:
                self.computeBase() # recompute the base resource
        else:
            self.computeBase() # create the initial base resource

    # Add a new change entry to the log. Entries are maintained in the order in which they're added.
    # changed: the URI of the resource that has changed.
    # type: the type of change (CREATION, MODIFICATION, or DELETION).
    # order: the sequence number for the new entry or None for the builder to generate one.
    # return: the sequence number assigned to the new entry.
    def addChangeEntry(self, changed, kind, order=None):
        if order is None:
            if len(self.currentChanges) != 0:
                order = self.currentChanges[len(self.currentChanges)-1].order + 1
            else:
                order = 1
        identifier = generateIdentifier(order)
        entry = ChangeEntry(kind, changed, order, identifier)
        self.currentChanges.append(entry)
        if len(self.currentChanges) == 2 * self.segmentSize: # Time to split into 2 segments?
            self.storeSegment()
            self.currentChanges = self.currentChanges[self.segmentSize:]
            self.segmentNo += 1
        self.storeSegment()
        return order

    # Get the latest Change Event in the ChangeLog.
    # return: a Change Event URI or null, if the log is empty.
    def getLatestChangeEvent(self):
        if len(self.currentChanges) != 0:
            return self.currentChanges[len(self.currentChanges)-1].identifier
        else:
            return RDF['nil']
    
    # Extract the segment number from a ChangeLogSegment URI.
    # segmentURI: the segment URI.
    # return: the segment number of an older segment or 0 for the current/latest segment.
    def extractSegmentNumber(self, segmentURI):
        if self.trackedResourceSetURI == segmentURI:
            return 0 
        else: 
            return int(segmentURI[len(self.changeLogURIBase)])
    
    # Store/persist the current segment in a file.
    def storeSegment(self):
        # TODO Re-implement this method in a way that doesn't regenerate the whole segment every time.
        graph = Graph()
        nextSegment = generateSegment(self.currentChanges, True, self.changeLogURIBase, self.segmentNo, self.segmentNo - 1, self.segmentSize, graph)
        if not isinstance(nextSegment, BNode):
            storeGraph(nextSegment, graph)
            return
        
        trackedResourceSet = URIRef(self.trackedResourceSetURI)
        graph.add((trackedResourceSet, TRS['changeLog'], nextSegment))
        graph.add((trackedResourceSet, TRS['base'], URIRef(self.currentBaseURI)))
        graph.add((trackedResourceSet, RDF['type'], TRS['TrackedResourceSet']))

        storeGraph(self.trackedResourceSetURI, graph);
        
    def initCurrentChanges(self, changeLogGraph, changeLogURI):
        listEntry = changeLogGraph.objects(changeLogURI, TRS['changes']).next()
        while listEntry != RDF['nil']:
            change = changeLogGraph.objects(listEntry, RDF['first']).next()
            kind = changeLogGraph.objects(change, RDF['type']).next()
            resource = changeLogGraph.objects(change, TRS['changed']).next()
            order = changeLogGraph.objects(change, TRS['order']).next()
            identifier = change;
            self.currentChanges.insert(0, ChangeEntry(kind, resource, order, identifier))
            listEntry = changeLogGraph.objects(listEntry, RDF['rest']).next()
        try: 
            nextSegmentURI = changeLogGraph.objects(changeLogURI, TRS['previous']).next()
            self.segmentNo = self.extractSegmentNumber(nextSegmentURI) + 1;
        except StopIteration: 
            return
        
    def computeBase(self):
        base_id = BASE_DOCUMENT_ID_PREFIX + operation_primitives.make_objectid()
        baseURI = URIRef(self.trackedResourceSetURIBase + base_id)
        graph = Graph()
        
        # get the cutoff event
        cutoffEvent = self.getLatestChangeEvent()
        graph.add((baseURI, TRS['cutoffEvent'], cutoffEvent))
        
        # compute the members
        cursor = operation_primitives.MONGO_DB[self.collectionName].find(fields={ '_id': True })
        while True:
            try: document = cursor.next()
            except StopIteration: break
            document_id = document['_id']
            member = url_policy.construct_url(self.publicHostname, None, self.collectionName, document_id)
            graph.add((baseURI, RDFS['member'], URIRef(member)))

        # store the new base
        storeGraph(baseURI, graph);
        
        # set the new base reference and store the TRS
        self.currentBaseURI = baseURI;
        self.storeSegment()
        
    #def is_system_document(self, document_id):
    #    return document_id == TRS_DOCUMENT_ID or \
    #           document_id.startswith(CL_DOCUMENT_ID_PREFIX) or \
    #           document_id.startswith(BASE_DOCUMENT_ID_PREFIX) or \
    #           document_id.startswith('new-')
        
if __name__ == '__main__':
    builder = TrackedResourceSetBuilder('localhost:3000', 'cm')
    #builder.computeBase()
    builder.addChangeEntry('http://someResourceUri', TRS['Modification'])
    builder.addChangeEntry('http://someResourceUri', TRS['Creation'])
    builder.getResource('http://example.com/cm/trs', 'text/turtle', sys.stdout)
