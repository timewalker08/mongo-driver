from iu_mongo.document import Document, EmbeddedDocument
from iu_mongo.fields import *
from iu_mongo.index import IndexDefinition
from datetime import datetime


class TestADoc(EmbeddedDocument):
    test_date = DateTimeField(default=datetime.utcnow, required=True)


class TestEDoc(EmbeddedDocument):
    test_int = IntField(required=True)
    test_timelist = EmbeddedDocumentListField('TestADoc')
    test_time = EmbeddedDocumentField('TestADoc')


class TestDoc(Document):
    meta = {
        'db_name': 'test',
        'indexes': [
            {'keys': 'test_int:1'},
            {'keys': 'test_pk:-1,test_int:1'},
            {'keys': 'test_int:1,test_list:1', 'unique': True},
            {'keys': 'test_pk:-1', 'unique': True},
            {'keys': 'test_dict:1', 'sparse': True},
            {'keys': 'test_list:1', 'expire_after_seconds': 10},
            {'keys': 'test_pk:1,test_int:1', 'unique': True},
        ]
    }
    test_int = IntField()
    test_str = StringField()
    test_pk = IntField(required=True)
    test_list = ListField(IntField())
    test_edoc = EmbeddedDocumentField('TestEDoc')
    test_dict = DictField()
    test_list_edoct = EmbeddedDocumentListField('TestEDoc')
