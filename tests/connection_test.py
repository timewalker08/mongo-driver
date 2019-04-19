import unittest
import pymongo
import mongomock
from iu_mongo.connection import connect, get_db, get_connection, clear_all, get_admin_db
from iu_mongo import Document
from iu_mongo.fields import IntField
from iu_mongo.errors import OperationError


class ConnectionTests(unittest.TestCase):
    def tearDown(self):
        clear_all()

    def test_connection(self):
        conn1 = connect(
            host='localhost',
            conn_name='conn1',
            db_names=['db1', 'db2']
        )
        conn2 = connect(
            host='localhost',
            conn_name='conn2',
            db_names=['db3', 'db4'],
            is_mock=True
        )
        # default connection name is 'main'
        conn3 = connect(
            host='127.0.0.1',
            db_names=['db5', 'db6'],
        )
        self.assertIsInstance(conn1, pymongo.mongo_client.MongoClient)
        self.assertIsInstance(conn2, mongomock.mongo_client.MongoClient)
        self.assertIsInstance(conn3, pymongo.mongo_client.MongoClient)
        conn1 = get_connection('conn1')
        conn2 = get_connection('conn2')
        conn3 = get_connection()
        self.assertIsInstance(conn1, pymongo.mongo_client.MongoClient)
        self.assertIsInstance(conn2, mongomock.mongo_client.MongoClient)
        self.assertIsInstance(conn3, pymongo.mongo_client.MongoClient)
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
        self.assertEqual(db1.client, conn1)
        self.assertEqual(db2.client, conn1)
        self.assertEqual(db3.client, conn2)
        self.assertEqual(db4.client, conn2)
        self.assertEqual(db5.client, conn3)
        self.assertEqual(db6.client, conn3)

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
                          conn2.list_databases)
        conn3 = connect(
            conn_name='conn3',
            username='right_username',
            password='right_password',
            auth_db='wrong_auth_db'
        )
        self.assertRaises(pymongo.errors.OperationFailure,
                          conn3.list_databases)
        conn4 = connect(
            conn_name='conn4',
            username='right_username',
            password='right_password'
        )
        conn4.list_databases()

    def test_connection_pool(self):
        conn = connect(max_pool_size=321)
        self.assertEqual(conn.max_pool_size, 321)

    def test_write_concern(self):
        class Doc1(Document):
            meta = {
                'db_name': 'test1',
            }
            test_int = IntField()

        class Doc2(Document):
            meta = {
                'db_name': 'test2',
                'write_concern': 3
            }
            test_int = IntField()

        conn1 = connect(
            conn_name='conn1',
            w=1
        )
        admin = get_admin_db('conn1')
        self.assertEqual(conn1.write_concern.document['w'], 1)
        self.assertEqual(admin.write_concern.document['w'], 1)
        conn2 = connect(
            conn_name='conn2',
            w=2,
            db_names=['test1']
        )
        self.assertEqual(conn2.write_concern.document['w'], 2)
        self.assertEqual(Doc1._pymongo().write_concern.document['w'], 2)
        doc1 = Doc1(test_int=1)
        self.assertRaises(OperationError, doc1.save)
        conn3 = connect(
            conn_name='conn3',
            db_names=['test2']
        )
        self.assertEqual(conn3.write_concern.document['w'], 'majority')
        self.assertEqual(
            Doc2._pymongo().write_concern.document['w'], 3)
        doc2 = Doc2(test_int=2)
        self.assertRaises(OperationError, doc2.save)
        clear_all()

        connect(db_names=['test1'])
        doc1.save()
        doc1.set(test_int=-10)
        Doc1.remove({})
