import unittest
from tests.model.testdoc import TestDoc
from mongo_driver.connection import connect, clear_all


class BulkTests(unittest.TestCase):
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

    def test_empty_bulk(self):
        with TestDoc.bulk():
            pass

    def test_bulk_save_exception(self):
        self._clear()
        try:
            self._feed_data(100, exception=True)
        except Exception:
            pass
        self.assertTrue(TestDoc.count({}) == 0)

    def test_bulk_save(self):
        limit = 100
        self._clear()
        self._feed_data(limit)
        self.assertTrue(TestDoc.count({}) == limit)

    def test_bulk_update_upsert(self):
        self._clear()
        with TestDoc.bulk() as bulk_context:
            for i in range(100):
                TestDoc.bulk_update(bulk_context, {'test_pk': i}, {'$set': {
                    'test_int': i,
                    'test_str': str(i),
                }}, upsert=True)
        self.assertTrue(TestDoc.count({}) == 100)

    def test_bulk_update_multi(self):
        limit = 100
        self._clear()
        self._feed_data(limit)
        with TestDoc.bulk() as bulk_context:
            for i in range(limit // 10):
                TestDoc.bulk_update(bulk_context, {
                    'test_pk': {'$lt': 10 * (i + 1), '$gt': 10 * i}
                }, {
                    '$set': {
                        'test_int': 1000
                    }
                }, multi=False)
        self.assertEqual(TestDoc.count({'test_int': 1000}), limit // 10)

    def test_bulk_remove(self):
        limit = 100
        self._clear()
        self._feed_data(100)
        with TestDoc.bulk() as bulk_context:
            for i in range(limit // 10):
                TestDoc.bulk_remove(bulk_context, {
                    'test_pk': {'$lt': 10 * (i + 1), '$gte': 10 * i}
                }, multi=False)
        self.assertEqual(TestDoc.count({}), limit - limit // 10)

    def test_bulk_update_one(self):
        self._clear()
        self._feed_data(100)
        docs = TestDoc.find({})
        with TestDoc.bulk() as bulk_context:
            for doc in docs:
                if doc.test_pk < 10:
                    doc.bulk_set(
                        bulk_context, test_int=doc.test_pk * doc.test_pk)
                elif doc.test_pk < 20:
                    doc.bulk_unset(bulk_context, test_int=True)
                elif doc.test_pk < 30:
                    doc.bulk_inc(bulk_context, test_int=2)
                elif doc.test_pk < 40:
                    doc.bulk_push(bulk_context, test_list=1000)
                elif doc.test_pk < 50:
                    doc.bulk_pull(bulk_context, test_list=doc.test_pk)
                else:
                    doc.bulk_add_to_set(
                        bulk_context, test_list=doc.test_pk * doc.test_pk)
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
