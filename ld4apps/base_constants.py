import os, importlib

RDFS = 'http://www.w3.org/2000/01/rdf-schema#'
RDF = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
LDP = 'http://www.w3.org/ns/ldp#'
XSD = 'http://www.w3.org/2001/XMLSchema#'
DC = 'http://purl.org/dc/terms/'
CE = 'http://ibm.com/ce/ns#'
OWL = 'http://www.w3.org/2002/07/owl#'
VCARD = 'http://www.w3.org/2006/vcard/ns#'
FOAF = 'http://xmlns.com/foaf/0.1/'
TRS = 'http://jazz.net/ns/trs#'
AC = 'http://ibm.com/ce/ac/ns#'
ENV = 'http://ibm.com/ce/env/ns#'

NAMESPACE_MAPPINGS = {
    RDFS: 'rdfs',
    RDF:  'rdf', 
    LDP:  'ldp',
    XSD:  'xsd',
    DC:   'dc',
    CE:   'ce',
    OWL:  'owl',
    AC:   'ac', # TODO: consider changing ac:resource-group to ce:resource-group, and then remove this line
    ENV:  'env'
    }

AC_T = 0x01 # Transform
AC_R = 0x02 # Read
AC_C = 0x04 # Create
AC_D = 0x08 # Delete
AC_W = 0x10 # Write
AC_X = 0x20 # Execute
AC_A = 0x40 # Admin
AC_ALL = AC_T|AC_R|AC_C|AC_D|AC_W|AC_X|AC_A

ADMIN_USER = 'http://ibm.com/ce/user/admin'
ANY_USER = 'http://ibm.com/ce/user/any'

# create and instance of a class from a #-separated module and class name (e.g., "some_module#SomeClass")
def create_instance(module_and_class_name):
    parts = module_and_class_name.split('#')
    module_name = parts[0]
    class_name = parts[1]
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_()

if 'URL_POLICY_CLASS' in os.environ:
    URL_POLICY = create_instance(os.environ['URL_POLICY_CLASS'])
else:
    #URL_POLICY = create_instance('url_policy#PathRootTenantURLPolicy')
    URL_POLICY = create_instance('ld4apps.url_policy#HostnameTenantURLPolicy')
