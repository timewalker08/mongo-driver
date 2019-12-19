import unittest
import pymongo
from bson import ObjectId
from iu_mongo import Document
from iu_mongo.fields import *
from iu_mongo.errors import OperationError
from iu_mongo.connection import connect, clear_all
from iu_mongo.errors import ConnectionError


class TestIndexDoc(Document):
    meta = {
        'db_name': 'test',
        'indexes': [
            {'keys': 'test_int:1', 'unique': True},
            {'keys': 'test_int:hashed'},
            {'keys': 'test_str:-1', 'sparse': True},
            {'keys': 'float:1'},
            {'keys': 'test_date:1', 'expire_after_seconds': 10},
            {
                'keys': 'test_int_p:1', 
                'unique': True,
                "partial_filter_expression": {
                    "test_int": {"$gt": 100}
                }
            }
        ]
    }
    test_int = IntField()
    test_str = StringField()
    test_float = FloatField()
    test_date = DateTimeField()
    test_int_p = IntField()


class IndexTests(unittest.TestCase):
    def setUp(self):
        try:
            connect(db_names=['test'])
        except ConnectionError:
            self.skipTest('Mongo service is not started localhost')

    def tearDown(self):
        clear_all()

    def _clear(self):
        TestIndexDoc.drop_collection()

    def test_index_creation(self):
        self._clear()
        coll = TestIndexDoc._pymongo()
        TestIndexDoc.create_indexes(confirm=False)
        indexes = TestIndexDoc.list_indexes(display=False)
        total = len(TestIndexDoc._meta['indexes'])+1
        self.assertEqual(len(indexes), total)
        # drop an already-exist index
        TestIndexDoc.drop_index('test_int_1')
        indexes = TestIndexDoc.list_indexes(display=False)
        indexes = filter(lambda x: x.built, indexes)
        self.assertEqual(len(list(indexes)), total-1)
        # drop a non-exist index
        with self.assertRaises(pymongo.errors.OperationFailure):
            TestIndexDoc.drop_index('test_date_-1')
