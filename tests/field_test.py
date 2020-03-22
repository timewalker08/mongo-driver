import unittest
import pymongo
import datetime
from bson import ObjectId
from mongo_driver import Document, connect
from mongo_driver.fields import *
from mongo_driver.errors import ValidationError
import mongo_driver


class Person(Document):
    meta = {
        'db_name': 'test'
    }
    name = StringField()
    age = IntField(default=30, required=False)
    userid = StringField(default=lambda: 'test', required=True)
    created = DateTimeField(default=datetime.datetime.utcnow)
    day = DateField(default=datetime.date.today)


class FieldTests(unittest.TestCase):
    def setUp(self):
        connect(db_names=['test'])

    def tearDown(self):
        Person.remove({})

    def test_default_not_set(self):
        person = Person(name="Ross")
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved,
                         ['age', 'created', 'day', 'name', 'userid']
                         )

        self.assertTrue(person.validate() is None)
        self.assertEqual(person.name, person.name)
        self.assertEqual(person.age, person.age)
        self.assertEqual(person.userid, person.userid)
        self.assertEqual(person.created, person.created)
        self.assertEqual(person.day, person.day)

        self.assertEqual(person._data['name'], person.name)
        self.assertEqual(person._data['age'], person.age)
        self.assertEqual(person._data['userid'], person.userid)
        self.assertEqual(person._data['created'], person.created)
        self.assertEqual(person._data['day'], person.day)

        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(
            data_to_be_saved, ['age', 'created', 'day', 'name', 'userid'])

    def test_default_set_none(self):
        person = Person(name=None, age=None, userid=None,
                        created=None, day=None)
        data_to_be_saved = sorted(person.to_mongo().keys())
        self.assertEqual(data_to_be_saved, ['age', 'created', 'day', 'userid'])

    def test_int_field(self):
        # max integer value mongodb can handle, i.e. 64-bit signed integer
        max_int_val = (1 << 63)-1

        class Doc(Document):
            meta = {
                'db_name': 'test'
            }
            test_int = IntField(min_value=-123, max_value=max_int_val)

        Doc.remove({})
        doc1 = Doc(test_int=max_int_val)
        doc2 = Doc(test_int=None)
        doc3 = Doc(test_int=max_int_val+1)
        doc4 = Doc(test_int=-200)
        doc1.save()
        doc2.save()
        self.assertEqual(Doc.count({'test_int': None}), 1)
        self.assertEqual(Doc.count({'test_int': {'$ne': None}}), 1)
        doc1 = Doc.find_one({'test_int': None})
        doc2 = Doc.find_one({'test_int': {'$ne': None}})
        self.assertEqual(doc1.test_int, None)
        self.assertEqual(doc2.test_int, max_int_val)
        self.assertRaises(mongo_driver.errors.ValidationError, doc3.save)
        self.assertRaises(mongo_driver.errors.ValidationError, doc4.save)

        doc5 = Doc(test_int='-123')
        doc5.save()
        self.assertEqual(Doc.count({'test_int': '-123'}), 0)
        doc5 = Doc.find_one({'test_int': -123})
        self.assertEqual(doc5.test_int, -123)
        # 32-bit signed type
        self.assertEqual(Doc.count({'test_int': {'$type': 'int'}}), 1)
        # 64-bit signed type
        self.assertEqual(Doc.count({'test_int': {'$type': 'long'}}), 1)
        Doc.remove({})

    def test_string_field(self):
        class Doc(Document):
            meta = {
                'db_name': 'test'
            }
            test_str = StringField()

        Doc.remove({})
        doc1 = Doc(test_str=None)
        doc2 = Doc(test_str='')
        doc3 = Doc(test_str='abcdefghij')
        doc4 = Doc(test_str='我')
        doc1.save()
        doc2.save()
        doc3.save()
        doc4.save()
        self.assertEqual(Doc.count({'test_str': None}), 1)
        self.assertEqual(Doc.count({'test_str': {'$ne': None}}), 3)
        self.assertEqual(Doc.count({'test_str': ''}), 1)
        doc4.reload()
        doc3.reload()
        self.assertIsInstance(doc3.test_str, str)
        self.assertIsInstance(doc4.test_str, str)
        self.assertEqual(Doc.count({'test_str': {'$type': 'string'}}), 3)
        Doc.remove({})
