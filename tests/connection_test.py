import unittest
import pymongo
import mongomock
from mongo_driver.connection import connect, get_db, get_connection, clear_all, \
    get_admin_db, DEFAULT_WRITE_CONCERN, DEFAULT_WTIMEOUT, DEFAULT_READ_CONCERN_LEVEL
from mongo_driver import Document
from mongo_driver.fields import IntField
from mongo_driver.errors import OperationError


class ConnectionTests(unittest.TestCase):
    def tearDown(self):
        clear_all()

    def test_connection(self):
        conn1 = connect(
            host='localhost',
            conn_name='conn1',
            db_names=['db1', 'db2']
        )
        self.assertEqual(conn1.name, 'conn1')
        conn2 = connect(
            host='localhost',
            conn_name='conn2',
            db_names=['db3', 'db4'],
            is_mock=True
        )
        self.assertEqual(conn2.name, 'conn2')
        # default connection name is 'main'
        conn3 = connect(
            host='127.0.0.1',
            db_names=['db5', 'db6'],
        )
        self.assertEqual(conn3.name, 'main')
        self.assertIsInstance(conn1.pymongo_client,
                              pymongo.mongo_client.MongoClient)
        self.assertIsInstance(conn2.pymongo_client,
                              mongomock.mongo_client.MongoClient)
        self.assertIsInstance(conn3.pymongo_client,
                              pymongo.mongo_client.MongoClient)
        conn1 = get_connection('conn1')
        conn2 = get_connection('conn2')
        conn3 = get_connection()
        self.assertIsInstance(conn1.pymongo_client,
                              pymongo.mongo_client.MongoClient)
        self.assertIsInstance(conn2.pymongo_client,
                              mongomock.mongo_client.MongoClient)
        self.assertIsInstance(conn3.pymongo_client,
                              pymongo.mongo_client.MongoClient)
        db1 = get_db('db1')
        db2 = get_db('db2')
        db3 = get_db('db3')
        db4 = get_db('db4')
        db5 = get_db('db5')
        db6 = get_db('db6')
        self.assertEqual(db1.name, 'db1')
        self.assertEqual(db2.name, 'db2')
        self.assertEqual(db3.name, 'db3')
        self.assertEqual(db4.name, 'db4')
        self.assertEqual(db5.name, 'db5')
        self.assertEqual(db6.name, 'db6')
        self.assertEqual(db1.client, conn1.pymongo_client)
        self.assertEqual(db2.client, conn1.pymongo_client)
        self.assertEqual(db3.client, conn2.pymongo_client)
        self.assertEqual(db4.client, conn2.pymongo_client)
        self.assertEqual(db5.client, conn3.pymongo_client)
        self.assertEqual(db6.client, conn3.pymongo_client)

    def test_authentication(self):
        conn1 = connect(
            conn_name='conn1'
        )
        admin = get_admin_db('conn1')
        admin.system.users.delete_many({})
        admin.command("createUser", "right_username",
                      pwd="right_password", roles=["dbOwner"])
        conn2 = connect(
            conn_name='conn2',
            username='wrong_username',
            password='wrong_password',
        )
        self.assertRaises(pymongo.errors.OperationFailure,
                          conn2.pymongo_client.list_databases)
        conn3 = connect(
            conn_name='conn3',
            username='right_username',
            password='right_password',
            auth_db='wrong_auth_db'
        )
        self.assertRaises(pymongo.errors.OperationFailure,
                          conn3.pymongo_client.list_databases)
        conn4 = connect(
            conn_name='conn4',
            username='right_username',
            password='right_password'
        )
        conn4.pymongo_client.list_databases()

    def test_connection_pool(self):
        conn = connect(max_pool_size=321)
        self.assertEqual(conn.pymongo_client.max_pool_size, 321)

    def test_write_concern(self):
        LARGE_W = 100

        class Doc1(Document):
            meta = {
                'db_name': 'test1',
            }
            test_int = IntField()

        class Doc2(Document):
            meta = {
                'db_name': 'test2',
                'write_concern': LARGE_W,
                'wtimeout': 5000,
            }
            test_int = IntField()

        conn1 = connect(
            conn_name='conn1',
            w=1
        )
        admin = get_admin_db('conn1')
        self.assertEqual(conn1.pymongo_client.write_concern.document['w'], 1)
        self.assertEqual(admin.write_concern.document['w'], 1)
        self.assertEqual(
            conn1.pymongo_client.write_concern.document['wtimeout'], DEFAULT_WTIMEOUT)
        self.assertEqual(
            admin.write_concern.document['wtimeout'], DEFAULT_WTIMEOUT)
        conn2 = connect(
            conn_name='conn2',
            w=LARGE_W,
            db_names=['test1'],
            wtimeout=2000,
        )
        self.assertEqual(
            conn2.pymongo_client.write_concern.document['w'], LARGE_W)
        self.assertEqual(Doc1._pymongo().write_concern.document['w'], LARGE_W)
        self.assertEqual(
            conn2.pymongo_client.write_concern.document['wtimeout'], 2000)
        self.assertEqual(
            Doc1._pymongo().write_concern.document['wtimeout'], 2000)
        doc1 = Doc1(test_int=1)
        self.assertRaises(OperationError, doc1.save)
        conn3 = connect(
            conn_name='conn3',
            db_names=['test2']
        )
        self.assertEqual(
            conn3.pymongo_client.write_concern.document['w'], DEFAULT_WRITE_CONCERN)
        self.assertEqual(
            Doc2._pymongo().write_concern.document['w'], LARGE_W)
        self.assertEqual(
            conn3.pymongo_client.write_concern.document['wtimeout'], DEFAULT_WTIMEOUT)
        self.assertEqual(
            Doc2._pymongo().write_concern.document['wtimeout'], 5000)
        doc2 = Doc2(test_int=2)
        self.assertRaises(OperationError, doc2.save)
        clear_all()

        connect(db_names=['test1'])
        doc1.save()
        doc1.set(test_int=-10)
        Doc1.remove({})

    def test_read_concern(self):
        class Doc(Document):
            meta = {
                'db_name': 'test'
            }

        conn = connect(db_names=['test'])
        pymongo_coll = Doc._pymongo()
        self.assertEqual(pymongo_coll.read_concern.level,
                         DEFAULT_READ_CONCERN_LEVEL)
        self.assertEqual(conn.pymongo_client.read_concern.level,
                         DEFAULT_READ_CONCERN_LEVEL)
