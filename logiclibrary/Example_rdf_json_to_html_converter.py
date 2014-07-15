import os, json
from rdf_json import URI, BNode, RDF_JSON_Encoder
import numbers, datetime
from base_constants import XSD, RDF

DEBUG_HTML = 'DEBUG_HTML' in os.environ and os.environ['DEBUG_HTML'] != 'False'

class Rdf_json_to_html_converter(object):

    def convert_graph_to_html(self, document, graph_id = None, g_indent=None):
        if g_indent is None:
            g_indent = '    '
        g_subject_indent = g_indent + '    '
        g_predicate_header_indent = g_subject_indent + '    '
        g_predicate_indent = (g_predicate_header_indent if DEBUG_HTML else g_subject_indent) + '    '
        if graph_id is None:
            graph_id = 'rdfa-graph'
        result = (g_indent + ('<div id = "%s" class="rdfa-graph" graph="%s"><br>\n' % (graph_id, document.graph_url) if DEBUG_HTML else '<div style="display: none;" graph="%s">\n' % document.graph_url))
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
                rslt = self.convert_graph_to_html(python_value['value'], graph_id + '-' + str(graph_count), predicate_indent + '    ')
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

    def convert_rdf_json_to_html(self, document):
        if not hasattr(document, 'graph_url'): # not an rdf_json document - probably an error condition
            return json.dumps(document, cls=RDF_JSON_Encoder)
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

            style = '''
        <style>
            .rdfa-graph { counter-reset: listing; }
            .rdfa-subject { counter-increment: listing; }
            .rdfa-predicate { counter-increment: listing; }
            .rdfa-subject:before { content: counter(listing) ". "; color: gray; }
            .rdfa-predicate:before { content: counter(listing) ". "; color: gray; }
        </style>
            body =
''' if DEBUG_HTML else ''
            result = '''
    <!DOCTYPE html>
    <html>
    <head>
        <script src="%s" type="text/javascript"></script>%s
    </head>
    <body>
%s
    </body>
    </html>''' % (application_url, style, self.convert_graph_to_html(document))

            return str(result)