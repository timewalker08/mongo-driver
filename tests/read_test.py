import unittest
import pymongo
import threading
import random
import time
from pymongo.write_concern import WriteConcern
from pymongo.errors import ConnectionFailure
from tests.model.testdoc import TestDoc
from iu_mongo.connection import connect, clear_all
from iu_mongo import SlaveOkSetting


class ReadTests(unittest.TestCase):
    def setUp(self):
        try:
            connect(db_names=['test'], conn_name='test', is_mock=True)
        except ConnectionError:
            self.skipTest('Mongo service is not started localhost')

    def _clear(self):
        TestDoc.remove({})

    def _feed_data(self, limit, exception=False):
        with TestDoc.bulk() as bulk_context:
            for i in range(limit):
                entry = TestDoc(test_int=i, test_str=str(i),
                                test_pk=i, test_list=[i])
                entry.bulk_save(bulk_context)
            if exception:
                raise Exception()

    def test_find(self):
        limit = 200
        self._clear()
        self._feed_data(limit)
        docs = TestDoc.find({}, limit=100)
        for doc in docs:
            self.assertEqual(doc.test_pk, doc.test_int)
            self.assertEqual(doc.test_pk, int(doc.test_str))
            self.assertEqual(doc.test_pk, doc.test_list[0])
        docs = TestDoc.find({}, limit=100, projection={'test_pk': True})
        for doc in docs:
            self.assertEqual(getattr(doc, 'test_int'), None)
            self.assertEqual(getattr(doc, 'test_str'), None)
            self.assertEqual(getattr(doc, 'test_list'), [])
        docs = TestDoc.find({}, limit=100, projection={
                            'test_pk': False, 'test_int': False, 'test_list': False})
        for doc in docs:
            self.assertEqual(getattr(doc, 'test_int'), None)
            self.assertEqual(getattr(doc, 'test_pk'), None)
            self.assertTrue(getattr(doc, 'test_str') is not None)
            self.assertEqual(getattr(doc, 'test_list'), [])

        docs = TestDoc.find({}, skip=10, limit=10, sort=[('test_pk', -1)])
        for index, doc in enumerate(docs):
            self.assertEqual(doc.test_pk, limit - index - 11)

        docs_iter = TestDoc.find_iter({}, batch_size=10, max_time_ms=200)
        for doc in docs_iter:
            self.assertEqual(doc.test_pk, doc.test_int)

    def test_distinct(self):
        limit = 100
        self._clear()
        self._feed_data(limit)
        docs = TestDoc.distinct({}, key='test_int')
        self.assertEqual(len(docs), limit)

        TestDoc.update({
            'test_pk': {'$lt': 10}
        }, {
            '$set': {
                'test_int': 1
            }
        })
        docs = TestDoc.distinct({}, key='test_int')
        self.assertEqual(len(docs), limit - 10 + 1)

    def test_reload(self):
        self._clear()
        self._feed_data(1)
        doc = TestDoc.find_one({})
        TestDoc.update(
            doc._update_one_key(),
            {
                '$set': {
                    'test_int': 1000
                }
            })
        self.assertEqual(doc.test_int, 0)
        doc.reload()
        self.assertEqual(doc.test_int, 1000)

    def test_aggregate(self):
        self._clear()
        self._feed_data(100)
        docs = list(TestDoc.aggregate([]))
        self.assertEqual(len(docs), 100)
        for doc in docs:
            self.assertIsInstance(doc, TestDoc)

    def test_read_preference(self):
        self._clear()
        self._feed_data(100)
        TestDoc.find({}, slave_ok=SlaveOkSetting.OFFLINE)

    def test_count(self):
        self._clear()
        self._feed_data(100)
        self.assertEqual(TestDoc.count(), 100)
        self.assertEqual(TestDoc.count({'test_pk': {'$lt': 10}}), 10)
        self.assertEqual(TestDoc.count({'test_pk': {'$lt': 10}}, skip=5), 5)
        self.assertEqual(TestDoc.count({'test_pk': {'$lt': 10}}, limit=3), 3)
        TestDoc.count({'test_int': {'$lt': 10}})
        # Should be warned
        # TestDoc.count({'test_int': {'$lt': 10}}, max_time_ms=-1)
        # TestDoc.count({'test_int': {'$lt': 10}}, max_time_ms=20000)
        # That's OK
        TestDoc.count({'test_int': {'$lt': 10}},
                      max_time_ms=-1, slave_ok=SlaveOkSetting.OFFLINE)
        doc = TestDoc.find_one({})
        self.assertEqual(TestDoc.count({'id': doc.id}), 1)

    def test_find_batch(self):
        self._clear()
        self._feed_data(1000)
        start = time.time()
        it = TestDoc.find_iter({}, sort=[('test_int', -1)], batch_size=10)
        for _ in it:
            pass
        end = time.time()
        time1 = end-start
        start = time.time()
        it = TestDoc.find_iter({}, sort=[('test_int', -1)], batch_size=100)
        for _ in it:
            pass
        end = time.time()
        time2 = end-start
        self.assertLess(time2, time1)

    def test_by_id(self):
        self._clear()
        self._feed_data(100)
        doc = TestDoc.find_one({'test_pk': 10})
        doc_id = doc.id
        new_doc = TestDoc.by_id(doc_id)
        self.assertEqual(new_doc.id, doc.id)
        new_doc = TestDoc.by_id(str(doc_id))
        self.assertEqual(new_doc.id, doc.id)

    def test_by_ids(self):
        self._clear()
        self._feed_data(10)
        docs = TestDoc.find({})
        doc_ids = [doc.id for doc in docs]
        doc_ids = doc_ids*2
        doc_ids_str = [str(x) for x in doc_ids]
        new_docs = TestDoc.by_ids(doc_ids)
        self.assertEqual(len(new_docs), 10)
        new_docs = TestDoc.by_ids(doc_ids_str)
        self.assertEqual(len(new_docs), 10)
