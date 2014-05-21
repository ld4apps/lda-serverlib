import os
import json
import rdf_json
from rdf_json import URI, BNode
from webob import Request
from rdfgraphlib import rdfjson_to_graph, serialize_graph
import Cookie
import datetime
import numbers
import utils
import jwt
from base_constants import XSD, RDF

# import logging
# logging.basicConfig(level=logging.DEBUG)

#logic_tier = importlib.import_module(os.environ['DOMAIN_LOGIC']) # importlib not in Python 2.6
if 'LOGIC_TIER' in os.environ:
    import_name = os.environ['LOGIC_TIER']
else:
    import_name = 'logic_tier' #assume it has the standard name and is on the python path
#logic_tier = importlib.import_module(import_name) # importlib not in Python 2.6
logic_tier = __import__(import_name, fromlist=['Domain_Logic'])
Domain_Logic = logic_tier.Domain_Logic

DEBUG_HTML = 'DEBUG_HTML' in os.environ and os.environ['DEBUG_HTML'] != 'False'

def post_document(environ, start_response):
    domain_logic = Domain_Logic(environ)
    post_reason = environ.get('HTTP_CE_POST_REASON')
    if not post_reason:
        post_reason = 'ce-action' # worst case - neither safe nor idempotent
    else:
        post_reason = post_reason.lower()
    if post_reason == 'ce-create' or post_reason == 'ce-transform' or post_reason == 'ce-action':
        content_type = environ.get('CONTENT_TYPE','').split(';')[0].lower()
        if content_type == 'multipart/form-data':
            req = Request(environ)
            document = req.POST
        else:
            request_body_size = int(environ.get('CONTENT_LENGTH', 0))
            request_body = environ['wsgi.input'].read(request_body_size)
            document = json.loads(request_body, object_hook = rdf_json.rdf_json_decoder)
        method = 'create_document' if post_reason == 'ce-create' else 'execute_query' if post_reason == 'ce-transform' else 'execute_action'
        status, headers, body = getattr(domain_logic, method)(document)
        add_standard_headers(environ, headers)
        if status == (201 if post_reason == 'ce-create' else 200):
            return make_json_response(status, headers, body, 'application/rdf+json+ce', start_response)
        elif status == 403:
            return send_auth_challenge(environ, start_response)
        else:
            return make_json_response(status, headers, body, 'application/json', start_response)
    elif post_reason == 'ce-patch':
        return patch_document(environ, start_response)
    else:
        return make_json_response(400, [], [('', 'unrecognized post reason %s' % post_reason)], 'application/json', start_response)

def get_document(environ, start_response):
    # In this application architectural style, the only method that ever returns HTML is GET. We never
    # return HTML from POST and we do not support application/x-www-form-urlencoded for POST
    domain_logic = Domain_Logic(environ)
    status, headers, body = domain_logic.get_document()
    add_standard_headers(environ, headers)
    request = Request(environ)
    best_match = request.accept.best_match(('text/html',
                                            'application/json',
                                            'application/rdf+json',
                                            'application/rdf+json+ce', # default
                                            'application/rdf+xml',
                                            'text/turtle',
                                            'application/x-turtle'))
    if status == 403:
        return send_auth_challenge(environ, start_response, best_match)
    elif status == 200:
        if not header_set('Content-Location', headers):
            host = utils.get_request_host(environ)
            if environ['QUERY_STRING']:
                content_location = 'http://%s%s?%s' %(host, environ['PATH_INFO'], environ['QUERY_STRING'])
            else:
                content_location = 'http://%s%s' %(host, environ['PATH_INFO'])
            headers.append(('Content-Location', content_location))
        if not header_set('Cache-Control', headers):
            headers.append(('Cache-Control', 'no-cache'))
        if not header_set('Vary', headers):
            headers.append(('Vary', 'Accept, Cookie'))
    if best_match == 'text/html':
        body = convert_rdf_json_to_html(body)
        return make_text_response(status, headers, body, best_match, start_response)
    elif best_match == 'application/json':
        if not hasattr(body, 'graph_url'): # not an rdf_json document - probably an error condition
            body = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
        else:
            body = domain_logic.convert_rdf_json_to_compact_json(body)
        return make_json_response(status, headers, body, best_match, start_response)
    elif best_match == 'application/rdf+json':
        body = rdf_json.normalize(body)
        return make_json_response(status, headers, body, best_match, start_response)
    elif best_match == 'application/rdf+xml' or best_match == 'text/turtle' or best_match == 'application/x-turtle':
        body = convert_rdf_json_to_rdf_requested(body, best_match)
        return make_text_response(status, headers, body, best_match, start_response)
    else:
        return make_json_response(status, headers, body, 'application/rdf+json+ce' if status == 200 else 'application/json', start_response)

def delete_document(environ, start_response):
    domain_logic = Domain_Logic(environ)
    status, headers, body = domain_logic.delete_document()
    add_standard_headers(environ, headers)
    if status == 204:
        start_response('%s %s' % (str(status), http_status_codes[status]) , headers)
        return ['']
    elif status == 403:
        return send_auth_challenge(environ, start_response)
    else:
        return make_json_response(status, headers, body, 'application/json', start_response)

def patch_document(environ, start_response):
    domain_logic = Domain_Logic(environ)
    content_type = environ['CONTENT_TYPE'].split(';')[0]
    if content_type == 'application/json':
        content_length = int(environ['CONTENT_LENGTH'])
        request_body = environ['wsgi.input'].read(content_length)
        document = json.loads(request_body, object_hook = rdf_json.rdf_json_decoder)
        status, headers, body = domain_logic.patch_document(document)
        add_standard_headers(environ, headers)
        if status == 200:
            return make_json_response(status, headers, body, 'application/rdf+json+ce', start_response)
        elif status == 403:
            return send_auth_challenge(environ, start_response)
        else:
            return make_json_response(status, headers, body, 'application/json', start_response)
    else:
        start_response('400 Bad Request', [])
        return ['content type of patch must be application/json, not %s' % content_type]

def put_document(environ, start_response):
    domain_logic = Domain_Logic(environ)
    content_type = environ['CONTENT_TYPE'].split(';')[0]
    if content_type == 'application/rdf+json+ce':
        content_length = int(environ['CONTENT_LENGTH'])
        request_body = environ['wsgi.input'].read(content_length)
        document = json.loads(request_body, object_hook = rdf_json.rdf_json_decoder)
        status, headers, body = domain_logic.put_document(document)
        add_standard_headers(environ, headers)
        if status == 201:
            return make_json_response(status, headers, body, 'application/rdf+json+ce', start_response)
        elif status == 403:
            return send_auth_challenge(environ, start_response)
        else:
            return make_json_response(status, headers, body, 'application/json', start_response)
    else:
        start_response('400 Bad Request', [])
        return ['content type of put must be application/rdf+json+ce, not %s' % content_type]

def explain_options(environ, start_response):
    headers = []
    origin = environ.get('HTTP_ORIGIN')
    if origin:
        headers.append(('Access-Control-Allow-Origin', origin))
        headers.append(('Access-Control-Allow-Methods', 'GET, OPTIONS, POST, DELETE, PATCH'))
        headers.append(('Access-Control-Allow-Credentials', 'true'))
        headers.append(('Access-Control-Allow-Headers', 'SSSESSIONID, If-Modified-Since, CE-Post-Reason, Content-Type'))
    start_response('200 OK', headers)
    return []

def application(environ, start_response):
    request_method = environ['REQUEST_METHOD']
    path_info = environ['PATH_INFO']
    path_parts = path_info.split('/')
    if request_method == 'GET':
        if path_parts[-1] == '__environ__':
            return get_environ(environ, start_response)
        elif path_parts[-1] == '__health__':
            return get_health(environ, start_response)
        else:
            return get_document(environ, start_response)
    elif request_method == 'POST':
        return post_document(environ, start_response)
    elif request_method == 'PATCH':
        return patch_document(environ, start_response)
    elif request_method == 'PUT':
        return put_document(environ, start_response)
    elif request_method == 'DELETE':
        return delete_document(environ, start_response)
    elif request_method == 'OPTIONS':
        return explain_options(environ, start_response)

    response_body = 'not handled - method: %s path: %s' % (environ['REQUEST_METHOD'], environ['PATH_INFO'])
    response_headers = [('Content-Type', 'text/plain'),
                       ('Content-Length', str(len(response_body)))]
    start_response('405 Method Not Allowed', response_headers)
    return [response_body]

def get_health(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain'), ('Content-length', '1')])
    return ['1']

def add_standard_headers(environ, headers):
    origin = environ.get('HTTP_ORIGIN')
    if origin and not header_set('Access-Control-Allow-Origin', headers):
        headers.append(('Access-Control-Allow-Origin', origin))
        headers.append(('Access-Control-Allow-Credentials', 'true'))
        headers.append(('Access-Control-Expose-Headers', 'Content-Location, Location'))
    if ('HTTP_SSSESSIONID' in environ):
        # user credentials from another domain were passed by the client
        session_key = environ['HTTP_SSSESSIONID']
        add_cookie = True
        cookie = Cookie.SimpleCookie()
        if ('HTTP_COOKIE' in environ):
            cookie.load(environ['HTTP_COOKIE'])
            if 'SSSESSIONID' in cookie:
                add_cookie = False
    elif ('SSSESSIONID' in environ):
        #  a JWT for an anonymous user URL was generated for an unauthenticated request or the JWT claims expired
        session_key = environ['SSSESSIONID']
        add_cookie = True
    else:
        add_cookie = False
    if add_cookie:
        cookie = Cookie.SimpleCookie()
        cookie['SSSESSIONID'] = session_key # SSSESSIONID  is 'Site Server Session ID'
        cookie['SSSESSIONID']['path'] = '/'
        claims = jwt.decode(session_key, verify=False)
        cookie['user'] = claims['user']
        cookie['user']['path'] = '/'
        cookie_headers = map(lambda morsel: ('Set-Cookie', morsel.OutputString()), cookie.values())
        headers.extend(cookie_headers)

def send_auth_challenge(environ, start_response, best_match_content_type='application/rdf+json+ce'):
    claims = utils.get_claims(environ)
    if claims is not None and 'acc' in claims:
        start_response('403 Forbidden', [])
        return ['access forbidden']
    else:
        original_url = '%s%s' % (environ['PATH_INFO'], '?%s'%environ['QUERY_STRING'] if environ['QUERY_STRING'] else '')
        if best_match_content_type == 'text/html':
            response_body = '<html><header><script>window.name = "%s";window.location.href = "/account/login"</script></header></html>' % original_url
        else:
            response_body = '{"http://ibm.com/ce/ns#login-page": "/account/login"}'
        response_headers = [('WWW-Authenticate', 'x-signature-auth realm=%s' % environ['SERVER_NAME']),('Content-Type', best_match_content_type), ('Content-Length', str(len(response_body)))]
        origin = environ.get('HTTP_ORIGIN')
        if origin:
            response_headers.append(('Access-Control-Allow-Origin', origin))
            response_headers.append(('Access-Control-Allow-Credentials', 'true'))
            response_headers.append(('Access-Control-Expose-Headers', 'Content-Location'))
        start_response('401 Unauthorized', response_headers)
        return [response_body]

def get_environ(environ, start_response):
    response_body = ['%s: %s' % (key, value)
                     for key, value in sorted(environ.items())]
    response_body = '\n'.join(response_body)

    # Response_body has now more than one string
    response_body = ['environ dictionary\n',
                    '*' * 30 + '\n',
                    response_body,
                    '\n' + '*' * 30 ,
                    '\nenviron dictionary end']
    # So the content-length is the sum of all string's lengths
    content_length = 0
    for s in response_body:
        content_length += len(s)

    response_headers = [('Content-Type', 'text/plain'),
                        ('Content-Length', str(content_length))]
    start_response('200 OK', response_headers)
    return response_body

def header_set(header, headers):
    headerl = header.lower()
    for item in headers:
        if item[0].lower() == headerl:
            return True
    return False

def make_json_response(status, headers, body, content_type, start_response):
    response_str = json.dumps(body, cls=rdf_json.RDF_JSON_Encoder)
    return make_text_response(status, headers, response_str, content_type, start_response)

def make_text_response(status, headers, body, content_type, start_response):
    if not header_set('Content-Type', headers):
        headers.append(('Content-Type', content_type))
    if not header_set('Cache-Control', headers):
        headers.append(('Cache-Control', 'no-cache'))
    headers.append(('Content-length', str(len(body))))
    start_response('%s %s' % (str(status), http_status_codes[status]), headers)
    return [body]

def convert_graph_to_html(document, graph_id = None, g_indent=None):
    if g_indent is None:
        g_indent = '    '
    g_subject_indent = g_indent + '    '
    g_predicate_header_indent = g_subject_indent + '    '
    g_predicate_indent = (g_predicate_header_indent if DEBUG_HTML else g_subject_indent) + '    '
    if graph_id is None:
        graph_id = 'rdfa-graph'
    result = (g_indent + '<div id = "%s" class="rdfa-graph" graph="%s"><br>\n' % (graph_id, document.graph_url)) if DEBUG_HTML else '<div style="display: none;" graph="%s">\n' % (document.graph_url)
    if DEBUG_HTML:
        result += g_subject_indent + '<h1>Graph: <a href="%s">%s</a></h1>\n' % (document.graph_url,document.graph_url)
    graph_count = 0
    def html_element(predicate, python_value, indent):
        property_string = 'property="%s" ' % predicate if predicate else ''
        class_string = 'class="rdfa-triple" ' if DEBUG_HTML else ''
        subject_indent = indent + '    '
        predicate_header_indent = subject_indent + '    '
        predicate_indent = (predicate_header_indent if DEBUG_HTML else subject_indent) + '    '
        if isinstance(python_value, (list, tuple)):
            rslt = predicate_indent + \
                '<span %s%sdatatype="%s"><br>\n' % (class_string, property_string, RDF+'List')
            subsequent = False
            for a_value in python_value:
                if DEBUG_HTML and subsequent:
                    rslt += '    ,'
                else:
                    subsequent = True
                rslt += html_element(None, a_value, indent + '    ')
            rslt += predicate_indent + '</span><br>\n'
        elif isinstance(python_value, URI) or isinstance(python_value, BNode):
            anchor_text = str(python_value) if DEBUG_HTML else ''
            rslt = predicate_indent + \
                '<a    %s%shref="%s">%s</a><br>\n' % (class_string, property_string, str(python_value), anchor_text)
        elif python_value is True or python_value is False:
            rslt = predicate_indent + \
                '<span %s%sdatatype="%s">%s</span><br>\n' % (class_string, property_string, XSD+'boolean', 'true' if python_value else 'false')
        elif isinstance(python_value, numbers.Number):
            if isinstance(python_value, numbers.Integral):
                rslt = predicate_indent + \
                    '<span %s%sdatatype="%s">%s</span><br>\n' % (class_string, property_string, XSD+'integer', python_value)
            else:
                rslt = predicate_indent + \
                    '<span %s%sdatatype="%s">%s</span><br>\n' % (class_string, property_string, XSD+'double', python_value)
        elif isinstance(python_value, basestring):
            rslt = predicate_indent + \
                '<span %s%s>%s</span><br>\n' % (class_string, property_string, str(python_value))
        elif isinstance(python_value, datetime.datetime):
            rslt = predicate_indent + \
                '<span %s%sdatatype="%s">%s</span><br>\n' % (class_string, property_string, XSD+'dateTime', python_value.isoformat())
        elif hasattr(python_value, 'keys') and python_value.get('type') == 'graph': #used to return versions
            rslt = predicate_indent + \
                '<span %sdatatype = "graph" property="%s" %s>{\n' % (class_string)
            rslt = convert_graph_to_html(python_value['value'], graph_id + '-' + str(graph_count), predicate_indent + '    ')
            graph_count = graph_count+1
            rslt = predicate_indent + '}</span><br>\n'
        elif python_value is None: # TODO: Review with Martin, figure out proper way to handle None value
            rslt = predicate_indent + \
                '<span %s%s%s>%s</span><br>\n' % (class_string, property_string, "BAD VALUE: NULL!")
        else:
            raise ValueError('new case? : %s' % python_value)
        return rslt
    for subject, rdf_json_subject_node in document.iteritems():
        if DEBUG_HTML and subject == document.graph_url:
            result += g_subject_indent + '<div class="rdfa-description" resource="%s" style="COLOR: red;">\n' % subject
        else:
            result += g_subject_indent + (('<div class="rdfa-description" resource="%s">\n' % subject) if DEBUG_HTML else \
                                 '<div resource="%s">\n' % subject)
        if DEBUG_HTML:
            result += g_subject_indent + '<h2>Subject: <a class = "rdfa-subject" href="%s">%s</a></h2>\n' % (subject, subject)
        for predicate, value_array in rdf_json_subject_node.iteritems():
            if DEBUG_HTML:
                result += g_predicate_header_indent + '<span class="rdfa-predicate"><b>%s:&nbsp;&nbsp;</b></span>\n' % predicate
            result += html_element(predicate, value_array, g_indent)
        result += g_indent + '    </div>\n'
    result += g_indent + '</div>'
    return result

def convert_rdf_json_to_html(document):
    if not hasattr(document, 'graph_url'): # not an rdf_json document - probably an error condition
        return json.dumps(document, cls=rdf_json.RDF_JSON_Encoder)
    else:
        config_file_name = '/etc/ce/conf.d/%s.conf' % os.environ['APP_NAME']
        try:
            config_file = open(config_file_name)
            application_url = config_file.read()
        except IOError as e:
            # print "Cannot open config file {2} - I/O error({0}): {1}".format(e.errno, e.strerror, config_file_name)
            application_url = '/%s/application.js' % os.environ['APP_NAME']
        else:
            config_file.close()

        result = ('''
<!DOCTYPE html>
<html>
<head>
    <title></title>
    <script src="%s" type="text/javascript"></script>
    <style>
        .rdfa-graph { counter-reset: listing; }
        .rdfa-subject { counter-increment: listing; }
        .rdfa-predicate { counter-increment: listing; }
        .rdfa-subject:before { content: counter(listing) ". "; color: gray; }
        .rdfa-predicate:before { content: counter(listing) ". "; color: gray; }
    </style>
</head>
<body>
    ''' if DEBUG_HTML else '''
<!DOCTYPE html>
<html>
<head>
    <script src="%s" type="text/javascript"></script>
</head>
<body>
    ''') % application_url
        result += convert_graph_to_html(document)
        result += '''
</body>
</html>'''

        return str(result)

def convert_rdf_json_to_rdf_requested(body, content_type):
    graph = rdfjson_to_graph(rdf_json.normalize(body))
    return serialize_graph(graph, content_type, None) #TODO: should we use wfile instead of string return value?

# Table mapping response codes to messages; entries have the
# form {code: 'reason'}. See RFC 2616.
http_status_codes = \
    {100: 'Continue',
     101: 'Switching Protocols',
     200: 'OK',
     201: 'Created',
     202: 'Accepted',
     203: 'Non-Authoritative Information',
     204: 'No Content',
     205: 'Reset Content',
     206: 'Partial Content',
     300: 'Multiple Choices',
     301: 'Moved Permanently',
     302: 'Found',
     303: 'See Other',
     304: 'Not Modified',
     305: 'Use Proxy',
     307: 'Temporary Redirect',
     400: 'Bad Request',
     401: 'Unauthorized',
     402: 'Payment Required',
     403: 'Forbidden',
     404: 'Not Found',
     405: 'Method Not Allowed',
     406: 'Not Acceptable',
     407: 'Proxy Authentication Required',
     408: 'Request Timeout',
     409: 'Conflict',
     410: 'Gone',
     411: 'Length Required',
     412: 'Precondition Failed',
     413: 'Request Entity Too Large',
     414: 'Request-URI Too Long',
     415: 'Unsupported Media Type',
     416: 'Requested Range Not Satisfiable',
     417: 'Expectation Failed',
     500: 'Internal Server Error',
     501: 'Not Implemented',
     502: 'Bad Gateway',
     503: 'Service Unavailable',
     504: 'Gateway Timeout',
     505: 'HTTP Version Not Supported'}
