import unittest
import pymongo
from bson import ObjectId
from iu_mongo.errors import OperationError
from tests.model.testdoc import *
from iu_mongo.connection import connect, clear_all
from iu_mongo.errors import ConnectionError


class WriteTests(unittest.TestCase):
    def setUp(self):
        try:
            connect(db_names=['test'])
        except ConnectionError:
            self.skipTest('Mongo service is not started localhost')

    def tearDown(self):
        clear_all()

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

    def test_save(self):
        self._clear()
        for i in range(10):
            doc = TestDoc(test_pk=i)
            pk_value = doc.save()
            self.assertEqual(TestDoc.count({'test_pk': i}), 1)
            self.assertIsInstance(pk_value, ObjectId)
        doc = TestDoc(id=pk_value, test_pk=3, test_int=2)
        try:
            doc.save()
        except OperationError:
            pass
        else:
            self.fail()
        doc = TestDoc(id=pk_value)
        doc.reload()
        self.assertEqual(doc.test_pk, 9)

    def test_delete(self):
        self._clear()
        self._feed_data(10)
        docs = TestDoc.find({})
        for doc in docs:
            if doc.test_pk < 5:
                doc.delete()
        self.assertEqual(TestDoc.count({}), 5)

    def test_update(self):
        self._clear()
        self._feed_data(100)
        result = TestDoc.update({'test_pk': {'$gt': -1}}, {
            '$set': {
                'test_int': 1000
            }
        })
        self.assertEqual(TestDoc.count({'test_int': 1000}), 100)
        self.assertEqual(result['nModified'], 100)
        result = TestDoc.update({'test_pk': {'$gt': -1}}, {
            '$set': {
                'test_int': 1000 * 2
            }
        }, multi=False)
        self.assertEqual(TestDoc.count({'test_int': 1000 * 2}), 1)
        self.assertEqual(result['nModified'], 1)
        result = TestDoc.update({'test_pk': 101}, {
            '$set': {
                'test_int': 1000 * 3
            }
        }, upsert=True)
        self.assertIsInstance(result['upserted_id'], ObjectId)
        self.assertEqual(TestDoc.count({}), 101)

    def test_remove(self):
        self._clear()
        self._feed_data(100)
        result = TestDoc.remove({'test_pk': {'$lt': 50}})
        self.assertEqual(result['n'], 50)
        self.assertEqual(TestDoc.count({}), 50)
        result = TestDoc.remove({'test_pk': {'$gte': 50}}, multi=False)
        self.assertEqual(result['n'], 1)
        self.assertEqual(TestDoc.count({}), 49)

    def test_find_and_modify(self):
        self._clear()
        self._feed_data(100)
        doc = TestDoc.find_and_modify(
            {
                'test_pk': {'$lt': 10}
            },
            {
                '$set': {
                    'test_int': 1000
                }
            },
            sort=[
                ('test_pk', -1)
            ],
            projection={
                'test_int': 1
            }
        )
        self.assertEqual(doc.test_int, 9)
        self.assertEqual(TestDoc.count({'test_int': 1000}), 1)
        self.assertEqual(doc.test_str, None)
        doc = TestDoc.find_and_modify(
            {
                'test_pk': 101
            },
            {
                '$set': {
                    'test_int': 101
                }
            },
            sort=[
                ('test_pk', -1)
            ],
            projection={
                'test_int': 1
            },
            upsert=True,
        )
        self.assertEqual(TestDoc.count({'test_pk': 101}), 1)
        doc = TestDoc.find_and_modify(
            {
                'test_pk': {'$lt': 10}
            },
            {
                '$set': {
                    'test_int': 1000 * 2
                }
            },
            sort=[
                ('test_pk', -1)
            ],
            new=True,
        )
        self.assertEqual(doc.test_int, 1000 * 2)
        doc = TestDoc.find_and_modify(
            {},
            {},
            sort=[
                ('test_pk', -1)
            ],
            remove=True,
        )
        self.assertEqual(doc.test_pk, 101)
        self.assertEqual(TestDoc.count({}), 100)

    def test_update_one(self):
        self._clear()
        self._feed_data(100)
        docs = TestDoc.find({})
        for doc in docs:
            if doc.test_pk < 10:
                doc.set(test_int=doc.test_pk * doc.test_pk)
                self.assertEqual(doc.test_int, doc.test_pk * doc.test_pk)
            elif doc.test_pk < 20:
                doc.unset(test_int=True)
                self.assertEqual(doc.test_int, None)
            elif doc.test_pk < 30:
                old = doc.test_int
                doc.inc(test_int=2)
                self.assertEqual(doc.test_int, old + 2)
            elif doc.test_pk < 40:
                doc.push(test_list=1000)
                self.assertIn(1000, doc.test_list)
            elif doc.test_pk < 50:
                doc.pull(test_list=doc.test_pk)
                self.assertNotIn(doc.test_pk, doc.test_list)
            else:
                doc.add_to_set(test_list=doc.test_pk * doc.test_pk)
                self.assertIn(doc.test_pk * doc.test_pk, doc.test_list)
        docs = TestDoc.find({})
        count1 = count2 = count3 = count4 = count5 = count6 = 0
        for doc in docs:
            if doc.test_int == doc.test_pk * doc.test_pk:
                count1 += 1
            elif doc.test_int is None:
                count2 += 1
            elif doc.test_int == doc.test_pk + 2:
                count3 += 1
            elif 1000 in doc.test_list:
                count4 += 1
            elif len(doc.test_list) == 0:
                count5 += 1
            elif doc.test_pk * doc.test_pk in doc.test_list:
                count6 += 1
        self.assertEqual(count1, 10)
        self.assertEqual(count2, 10)
        self.assertEqual(count3, 10)
        self.assertEqual(count4, 10)
        self.assertEqual(count5, 10)
        self.assertEqual(count6, 50)
        doc = TestDoc(test_pk=101, test_int=101)
        doc.set(test_int=12)
        self.assertEqual(doc.test_int, 101)
        doc = TestDoc(id=docs[0].id)
        doc.set(test_int=-1)
        self.assertEqual(doc.test_int, -1)

    def test_update_document_transform(self):
        import datetime
        self._clear()
        adoc = TestADoc()
        edoc = TestEDoc(
            test_int=1,
            test_time=adoc,
            test_timelist=[adoc]*3)
        doc = TestDoc(
            test_pk=1,
            test_edoc=edoc,
            test_list_edoct=[edoc]*3,
        )
        doc.save()
        self.assertEqual(len(doc.test_list_edoct), 3)
        self.assertEqual(len(doc.test_edoc.test_timelist), 3)
        adoc = TestADoc(test_date=datetime.datetime(2019, 1, 1))
        edoc.test_int = -5
        edoc.test_time = adoc
        edoc.test_timelist = [adoc]
        doc.set(test_edoc=edoc)
        doc.set(test_list_edoct=[edoc])
        self.assertEqual(len(doc.test_list_edoct), 1)
        self.assertEqual(len(doc.test_edoc.test_timelist), 1)
        with TestDoc.bulk() as ctx:
            edoc.test_int = -10
            doc.bulk_set(ctx, test_edoc=edoc)
            doc.bulk_set(ctx, test_list_edoct=[edoc])
        doc.reload()
        self.assertEqual(doc.test_edoc.test_int, -10)
        self.assertEqual(doc.test_list_edoct[0].test_int, -10)
