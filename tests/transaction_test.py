import unittest
import pymongo
import iu_mongo
from iu_mongo import Document, connect, clear_all
from iu_mongo.fields import *
from iu_mongo.session import DEFAULT_READ_CONCERN, DEFAULT_READ_PREFERENCE, DEFAULT_WRITE_CONCERN
from iu_mongo.errors import TransactionError
from iu_mongo.slave_ok_setting import SlaveOkSetting


class Doc(Document):
    meta = {
        'db_name': 'test'
    }
    test_int = IntField()
    name = StringField()
    test_push = ListField(IntField())
    test_set = ListField(IntField())
    test_pull = ListField(IntField())


class TransactionTests(unittest.TestCase):
    def setUp(self):
        try:
            connect(db_names=['test'])
        except ConnectionError:
            self.skipTest('Mongo service is not started localhost')

    def tearDown(self):
        clear_all()

    def test_configuration(self):
        class DocTest(Document):
            meta = {
                'db_name': 'test',
                # the following configuration will be ignored in transaction context
                'write_concern': 10,
                'wtimeout': 1000,
            }
            test_int = IntField()
            name = StringField()
        connection = DocTest.get_connection()
        session = connection.start_session()
        transaction_context = session.start_transaction()
        self.assertEqual(
            transaction_context._transaction.opts.read_concern, DEFAULT_READ_CONCERN)
        self.assertEqual(
            transaction_context._transaction.opts.write_concern, DEFAULT_WRITE_CONCERN)
        self.assertEqual(
            transaction_context._transaction.opts.read_preference, DEFAULT_READ_PREFERENCE)
        doc = DocTest(test_int=1)
        with connection.start_session() as session:
            self.assertRaises(iu_mongo.errors.OperationError, doc.save)
            self.assertRaises(iu_mongo.errors.OperationError,
                              doc.save, session=session)
            with session.start_transaction():
                self.assertRaises(iu_mongo.errors.OperationError, doc.save)
                DocTest.remove({}, session=session)
                doc.save(session=session)
                self.assertEqual(DocTest.count({}, session=session), 1)
            self.assertEqual(DocTest.count({}, session=session), 1)

    def test_exception(self):
        connection = Doc.get_connection()
        session = connection.start_session()
        session.start_transaction()
        self.assertRaises(TransactionError, session.start_transaction)
        session.abort_transaction()
        self.assertRaises(TransactionError, session.abort_transaction)

    def test_read_in_or_out_transaction(self):
        Doc.remove({})

        def test_func(session, raise_exception=False):
            Doc.update({
                'test_int': 1
            }, {
                '$set': {
                    'test_int': 2
                }
            }, upsert=True, session=session)
            # find
            result_1, result_2 = Doc.find(
                {}), Doc.find({}, session=session)
            # aggregate
            result_3, result_4 = list(Doc.aggregate(
                [])), list(Doc.aggregate([], session=session))
            self.assertEqual(len(result_1), 0)
            self.assertEqual(len(result_2), 1)
            # count
            self.assertEqual(Doc.count({}), 0)
            self.assertEqual(Doc.count({}, session=session), 1)
            self.assertEqual(len(result_3), 0)
            self.assertEqual(len(result_4), 1)
            self.assertEqual(result_4[0]['_id'], result_2[0].id)
            doc = result_2[0]
            Doc.update({
                'test_int': 2
            }, {
                '$set': {
                    'test_int': 3
                }
            }, session=session)
            if raise_exception:
                raise Exception()
            # reload, without session, won't get anything
            doc.reload()
            self.assertEqual(doc.test_int, 2)
            doc.reload(session=session)
            self.assertEqual(doc.test_int, 3)
            # by_ids
            result_1, result_2 = Doc.by_ids(
                [doc.id]), Doc.by_ids([doc.id], session=session)
            self.assertEqual(len(result_1), 0)
            self.assertEqual(len(result_2), 1)
            # by_id
            result_1, result_2 = Doc.by_id(
                doc.id), Doc.by_id(doc.id, session=session)
            self.assertEqual(result_2.id, doc.id)
            self.assertEqual(result_1, None)
            # find_one
            result_1, result_2 = Doc.find_one(
                {}), Doc.find_one({}, session=session)
            self.assertEqual(result_2.id, doc.id)
            self.assertEqual(result_1, None)
            # distinct
            result_1, result_2 = Doc.distinct({}, 'test_int'), Doc.distinct(
                {}, 'test_int', session=session)
            self.assertEqual(result_1, [])
            self.assertEqual(result_2, [3])
            # find_iter
            iter_1, iter_2 = Doc.find_iter(
                {}), Doc.find_iter({}, session=session)
            self.assertEqual(len(list(iter_1)), 0)
            self.assertEqual(len(list(iter_2)), 1)

        with Doc.get_connection().start_session() as session:
            # test case when exception raised in transaction, we shall get nothing
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}), 0)
            self.assertEqual(Doc.count({}, session=session), 0)
            # test case when exception raised in non-transaction mode, we will get first result
            try:
                test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}), 1)
            self.assertEqual(Doc.count({}, session=session), 1)
            doc = Doc.find_one({}, session=session)
            self.assertEqual(doc.test_int, 2)
            Doc.remove({})
            with session.start_transaction():
                test_func(session)
            doc = Doc.find_one({}, session=session)
            self.assertEqual(doc.test_int, 3)

            Doc.remove({})
            # test case not using 'with' keyword with transaction
            # will abort transaction after session ended
            session.start_transaction()
            test_func(session)
        self.assertEqual(Doc.count(), 0)

        def test_func2(session, raise_exception=False):
            A = Doc.find_one({'name': 'A'}, session=session)
            B = Doc.find_one({'name': 'B'}, session=session)
            A.inc(test_int=-50, _session=session)
            if raise_exception:
                raise Exception()
            B.inc(test_int=50, _session=session)

        with Doc.get_connection().start_session() as session:
            Doc.update({
                'name': 'A'
            }, {
                '$set': {
                    'test_int': 100
                }
            }, upsert=True, session=session)
            Doc.update({
                'name': 'B'
            }, {
                '$set': {
                    'test_int': 0
                }
            }, upsert=True, session=session)
            A = Doc.find_one({'name': 'A'}, session=session)
            B = Doc.find_one({'name': 'B'}, session=session)
            try:
                with session.start_transaction():
                    test_func2(session, raise_exception=True)
            except Exception:
                pass
            A.reload(session=session)
            B.reload(session=session)
            self.assertEqual(A.test_int, 100)
            self.assertEqual(B.test_int, 0)
            with session.start_transaction():
                test_func2(session)
            A.reload(session=session)
            B.reload(session=session)
            self.assertEqual((A.test_int, B.test_int), (50, 50))

    def test_update(self):
        def test_func(session, raise_exception=False):
            Doc.update({
                'test_int': 1
            }, {
                '$inc': {
                    'test_int': 1
                }
            }, session=session)
            Doc.update({
                'test_int': 2
            }, {
                '$inc': {
                    'test_int': 1
                }
            }, session=session)
            if raise_exception:
                raise Exception()
            Doc.update({
                'test_int': 3
            }, {
                '$inc': {
                    'test_int': 1
                }
            }, session=session)
        Doc.drop_collection()
        Doc(test_int=1).save()
        Doc(test_int=1).save()
        Doc(test_int=1).save()
        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}, session=session), 3)
            self.assertEqual(Doc.count({'test_int': 1}, session=session), 3)
            with session.start_transaction():
                test_func(session)
            self.assertEqual(Doc.count({}, session=session), 3)
            self.assertEqual(Doc.count({'test_int': 4}, session=session), 3)
        self.assertEqual(Doc.count(), 3)
        self.assertEqual(Doc.count({'test_int': 4}), 3)

    def test_find_and_modify(self):
        def test_func(session, raise_exception=False):
            Doc.find_and_modify({
                'test_int': 1
            }, {
                '$inc': {'test_int': 1}
            }, session=session)
            Doc.find_and_modify({
                'test_int': 2
            }, {
                '$inc': {'test_int': 1}
            }, session=session)
            if raise_exception:
                raise Exception()
            Doc.find_and_modify({
                'test_int': 3
            }, {
                '$inc': {'test_int': 1}
            }, session=session)
        Doc.drop_collection()
        Doc(test_int=1).save()
        Doc(test_int=1).save()
        Doc(test_int=1).save()
        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}, session=session), 3)
            self.assertEqual(Doc.count({'test_int': 1}, session=session), 3)
            with session.start_transaction():
                test_func(session)
            self.assertEqual(Doc.count({}, session=session), 3)
            self.assertEqual(Doc.count({'test_int': 4}, session=session), 1)
        self.assertEqual(Doc.count(), 3)
        self.assertEqual(Doc.count({'test_int': 4}), 1)

    def test_remove(self):
        Doc.drop_collection()
        Doc(test_int=1, test_list1=[]).save()
        Doc(test_int=1).save()
        Doc(test_int=1).save()

        def test_func(session, raise_exception=False):
            Doc.remove({'test_int': 1}, multi=False, session=session)
            Doc.remove({'test_int': 1}, multi=False, session=session)
            if raise_exception:
                raise Exception()
            Doc.remove({'test_int': 1}, multi=False, session=session)

        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}, session=session), 3)
            with session.start_transaction():
                test_func(session)
            self.assertEqual(Doc.count({}, session=session), 0)
        self.assertEqual(Doc.count(), 0)

    def test_save(self):
        Doc.drop_collection()
        # transaction must run while collection exists
        Doc.create_collection_if_not_exists()

        def test_func(session, raise_exception=False):
            id1 = Doc(test_int=1).save(session=session)
            id2 = Doc(test_int=1).save(session=session)
            if raise_exception:
                raise Exception()
            id3 = Doc(test_int=1).save(session=session)
            return [id1, id2, id3]

        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}, session=session), 0)
            with session.start_transaction():
                id1, id2, id3 = test_func(session)
            self.assertEqual(Doc.count({}, session=session), 3)
            for doc_id in [id1, id2, id3]:
                self.assertEqual(Doc.by_id(doc_id, session=session).id, doc_id)
        self.assertEqual(Doc.count(), 3)

    def test_delete(self):
        Doc.drop_collection()
        doc1 = Doc(test_int=1)
        doc2 = Doc(test_int=1)
        doc3 = Doc(test_int=1)
        doc1.save()
        doc2.save()
        doc3.save()

        def test_func(session, raise_exception=False):
            doc1.delete(session=session)
            doc2.delete(session=session)
            if raise_exception:
                raise Exception()
            doc3.delete(session=session)

        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count({}, session=session), 3)
            with session.start_transaction():
                test_func(session)
            self.assertEqual(Doc.count({}, session=session), 0)
        self.assertEqual(Doc.count(), 0)

    def test_update_one(self):
        Doc.drop_collection()
        doc1 = Doc(test_int=1, name='A', test_pull=[1, 2, 3], test_set=[2, 3])
        doc2 = Doc(test_int=1, name='B', test_pull=[1, 2, 3], test_set=[2, 3])
        doc3 = Doc(test_int=1, name='C', test_pull=[1, 2, 3], test_set=[2, 3])
        doc1.save()
        doc2.save()
        doc3.save()

        def test_func(session, raise_exception=False):
            doc1.unset(name=True, _session=session)
            doc2.unset(name=True, _session=session)
            doc3.unset(name=True, _session=session)
            if raise_exception:
                raise Exception()
            # set
            doc1.set(test_int=-1, _session=session)
            doc2.set(test_int=-2, _session=session)
            doc3.set(test_int=-3, _session=session)
            # push
            doc1.push(test_push=1, _session=session)
            doc2.push(test_push=2, _session=session)
            doc3.push(test_push=3, _session=session)
            # pull
            doc1.pull(test_pull=1, _session=session)
            doc2.pull(test_pull=2, _session=session)
            doc3.pull(test_pull=3, _session=session)
            # add_to_set
            doc1.add_to_set(test_set=1, _session=session)
            doc2.add_to_set(test_set=2, _session=session)
            doc3.add_to_set(test_set=3, _session=session)
        with Doc.get_connection().start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(
                Doc.count({'name': {'$exists': True}}, session=session), 3)
            self.assertEqual(Doc.count({'test_int': 1}, session=session), 3)
            self.assertEqual(
                Doc.count({'test_pull': [1, 2, 3]}, session=session), 3)
            self.assertEqual(
                Doc.count({'test_set': [2, 3]}, session=session), 3)
            self.assertEqual(
                Doc.count({'test_push': []}, session=session), 3)
            with session.start_transaction():
                test_func(session)
            self.assertEqual(
                Doc.count({'name': {'$exists': True}}, session=session), 0)
            for int_val in[-1, -2, -3]:
                self.assertEqual(
                    Doc.count({'test_int': int_val}, session=session), 1)
            self.assertEqual(
                Doc.count({'test_pull': {"$size": 2}}, session=session), 3)
            self.assertEqual(
                Doc.count({'test_set': {'$size': 3}}, session=session), 1)
            self.assertEqual(
                Doc.count({'test_set': {'$size': 2}}, session=session), 2)
            for int_val in [1, 2, 3]:
                self.assertEqual(
                    Doc.count({'test_push': [int_val]}, session=session), 1)
        self.assertEqual(Doc.count({'name': {'$exists': True}}), 0)

    def test_cross_collection_and_db(self):
        class CollA(Document):
            meta = {
                'db_name': 'test1'
            }
            test_int = IntField()

        class CollB(Document):
            meta = {
                'db_name': 'test2'
            }
            test_int = IntField()

        def test_func(session, raise_exception=False):
            CollA.update({}, {'$inc': {'test_int': 50}}, session=session)
            if raise_exception:
                raise Exception()
            CollB.update({}, {'$inc': {'test_int': -50}}, session=session)

        def check_consistency(test_instance, session=None):
            doc1 = CollA.find_one({}, session=session)
            doc2 = CollB.find_one({}, session=session)
            test_instance.assertEqual(doc1.test_int+doc2.test_int, 300)

        clear_all()
        connection = connect(db_names=['test1', 'test2'])
        CollA.drop_collection()
        CollB.drop_collection()
        CollA(test_int=100).save()
        CollB(test_int=200).save()
        with connection.start_session() as session:
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            check_consistency(self, session=session)
            with session.start_transaction():
                test_func(session)
            check_consistency(self, session=session)
        check_consistency(self)
        self.assertEqual(CollA.find_one({}).test_int, 150)
        self.assertEqual(CollB.find_one({}).test_int, 150)

    def test_bulk(self):
        Doc.drop_collection()

        def test_func(session, raise_exception=False):
            doc1 = Doc(test_int=1)
            doc2 = Doc(test_int=2)
            doc3 = Doc(test_int=3)
            with Doc.bulk(session=session) as ctx:
                doc1.bulk_save(ctx)
                doc2.bulk_save(ctx)
                doc3.bulk_save(ctx)
            if raise_exception:
                raise Exception()
            doc1 = Doc.find_one({'test_int': 1}, session=session)
            doc2 = Doc.find_one({'test_int': 2}, session=session)
            doc3 = Doc.find_one({'test_int': 3}, session=session)
            with Doc.bulk(session=session) as ctx:
                doc1.bulk_set(ctx, name='A')
                doc2.bulk_set(ctx, name='B')
                doc3.bulk_set(ctx, name='C')
        with Doc.get_connection().start_session() as session:
            with self.assertRaises(Exception):
                test_func(session, raise_exception=True)
            self.assertEqual(Doc.count(session=session), 3)
            for int_val in [1, 2, 3]:
                self.assertEqual(
                    Doc.count({'test_int': int_val, 'name': {'$exists': False}}, session=session), 1)
            Doc.remove({}, session=session)
            try:
                with session.start_transaction():
                    test_func(session, raise_exception=True)
            except Exception:
                pass
            self.assertEqual(Doc.count(session=session), 0)
            with session.start_transaction():
                test_func(session=session)
            self.assertEqual(Doc.count(session=session), 3)
            for val in [(1, 'A'), (2, 'B'), (3, 'C')]:
                self.assertEqual(
                    Doc.count({'test_int': val[0], 'name': val[1]}, session=session), 1)
