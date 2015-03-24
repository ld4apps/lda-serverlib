
class Domain_Logic(object):

    def __init__(self, environ, change_tracking=False):
        self.environ = environ
        self.logic_tier = None

    def create_logic_tier(self):
        if not self.logic_tier:
            raise ValueError('must override')
        return self.logic_tier

    def get_document(self):
        return self.create_logic_tier().get_document()
    def get_collection(self):
        return self.create_logic_tier().get_collection()
    def delete_document(self):
        return self.create_logic_tier().delete_document()
    def drop_collection(self):
        return self.create_logic_tier().drop_collection()
    def patch_document(self, patch):
        return self.create_logic_tier().patch_document(patch)
    def put_document(self, new_document):
        return self.create_logic_tier().put_document(new_document)
    def create_document(self, document):
        return self.create_logic_tier().create_document(document)
    def execute_query(self, query):
        return self.create_logic_tier().execute_query(query)
    def execute_action(self, query):
        return self.create_logic_tier().execute_action(query)

    def convert_compact_json_to_rdf_json(self, document):
        return self.create_logic_tier().convert_compact_json_to_rdf_json(document)    

    def convert_rdf_json_to_compact_json(self, document):
        return self.create_logic_tier().convert_rdf_json_to_compact_json(document)    

    def convert_rdf_json_to_html(self, document):
        return self.create_logic_tier().convert_rdf_json_to_html(document)  