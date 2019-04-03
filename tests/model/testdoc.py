from iu_mongo.document import Document, EmbeddedDocument
from iu_mongo.fields import *

class TestEDoc(EmbeddedDocument):
    test_int = IntField()


class TestDoc(Document):
    meta = {
        'db_name': 'test'
    }
    test_int = IntField()
    test_str = StringField()
    test_pk = IntField(required=True)
    test_list = ListField(IntField())
    test_edoc = EmbeddedDocumentField('TestEDoc')
    test_dict = DictField()
    test_list_edoct = EmbeddedDocumentListField('TestEDoc')
