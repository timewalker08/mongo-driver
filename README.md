About
=================

**iu_mongo** is a simple mongo driver based on [pymongo](https://docs.mongodb.com/ecosystem/drivers/python/) (the official mongodb driver for python language) and provide convinient features such like **Document Definition** and **Document Manipulation** just like [mongoengine](http://mongoengine.org/), and the codes are based on mongoengine(but are not dependent on mongoengine). The only dependence of **iu_mongo** is pymongo.

Supported pymongo version
================
pymongo with version 3.7+ is supported

Supported Python version
=======
python 3+ is supported

Supported MongoDB version
=======
MongoDB 4.0+ is supported

Installation
=====
just install iu_mongo as a VCS pip package installation in editable mode

    pip install -e git+git://github.com/intelligenceunion/mongo-driver.git

Connect to MongoDB
=======
```python
from iu_mongo import connect
connect(host="MONGODB_HOST",db_names=["DB_NAME"], username='username', password='password', auth_db='admin')
```

### Write Concern 
You can set keyword arguments `w(integer or string)` and `wtimeout(integer)` to set [Write Conern](https://docs.mongodb.com/manual/reference/write-concern/) in connection level   (i.e. default is `{'w':'majority','wtimeout':5000}`), by doing so, all dbs/collections under this connection will use this **write concern**, unless you specify it in meta dict of the document definition class explicitly, which will override the default **write concern** settings. For example
```python
from iu_mongo import Document
from iu_mongo import connect

class Doc(Document):
    meta={
        'db_name':'test',
        'write_concern':1,
        'wtimeout':1000,
    }

connect(db_names=['test'])
```
Even though the write concern of the connection is `{'w':'majority','wtimeout':5000}` the actual write concern of **Doc** is `{'w':1,'wtimeout':1000}` as specified in the Doc class

### Replica Set Configuration
You can pass `replica_set` keyword argument to specify the replica set you are connecting to if you mongodb deployment is a replica set.

### Examples
1. Your MongoDB deployment is a single mongod instance
   ```python
   from iu_mongo import connect
   connect(host='MONGOD_IP:PORT')
   ```
2. Your MongoDB deployment is a replica set
   ```python
   from iu_mongo import connect
   # recommended way
   connect(host="ANY_REPLICA_NODE_IP:PORT",replica_set="REPLICA_SET_NAME")
   ```
   or
   ```python
   from iu_mongo import connect
   # all node ips are needed if replica_set not specified
   connect(host=["NODE_1_IP:PORT","NODE_2_IP:PORT"])
   ```
3. Your MongoDB deployment is a shard cluster
   ```python
   from iu_mongo import connect
   # pass a mongos node list if you have multi mongos
   connect(host=['MONGOS_NODE:PORT'])
   ```
Document Definition
=============
**Document Definition** is very like to mongoengine. Still, little difference is between them.

```python
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
```
Note that thare are two main difference between iu_mongo and mongoengine when defining a document

1. `db_field` is never needed, iu_mongo will use the **field name** as the field name when saving documents into mongodb.
2. `primary_key` is not supported, please issue an unique index build if you need a primary key

Also, please specify `meta['db_name']` to a db name in which database iu_mongo will store documents for mongodb

Document Manipulation
====
iu_mongo provide many collection-level operations as well as document-level operations. Supported operations are

- find
- find_iter
- find_one
- count
- distinct
- aggregate
- reload
- update
- find_and_modify
- remove
- save
- delete
- update_one
- set, unset, inc, push, pull, add_to_set
- by_id, by_ids
- drop_collection  

Also, iu_mongo support bulk-like operations, for example
```python
    with TestDoc.bulk() as bulk_context:
        for i in range(10):
            TestDoc.bulk_update(bulk_context, {
                'test_pk': {'$lt': 10 * (i + 1), '$gt': 10 * i}
            }, {
                '$set': {
                    'test_int': 1000
                }
            }, multi=False)
```

supported bulk operations are
- bulk_save
- bulk_update
- bulk_remove
- bulk_update_one
- bulk_set
- bulk_inc
- bulk_push
- bulk_pull
- bulk_add_to_set

### Multi-document Transactions
Transaction is supported in iu_mongo, for example, say we have two document definition in the following example:
```python
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
```
Now let's assume there is one doc in both CollA and CollB
```python
CollA(test_int=100).save()
CollB(test_int=200).save()
```
Then a common transaction may be like the following:
```python
connection = connect(db_names=['test1', 'test2'])
with connection.start_session() as session:
    with session.start_transaction():
        CollA.update({}, {'$inc': {'test_int': 50}}, session=session)
        CollB.update({}, {'$inc': {'test_int': -50}}, session=session)
```
As a summary:
1. First, get a `connection` to mongodb (transactions can be cross collections/databases in one connection) 
2. Start a `session` from the connection(using `with`), mongodb provide **causal consistency** under one session, please refer [here](https://docs.mongodb.com/manual/core/causal-consistency-read-write-concerns/)
3. Start a `transaction` from the session(using `with`)
4. Issue your actions in transcation with the current session (pass `session` keyword argument to every action)
5. Transaction will commit automatically if no exception occurs
   


Refer to `transaction_test.py` for more examples of transactions.
Also, you can learn transactions from [MongoDB](https://docs.mongodb.com/manual/core/transactions/)


DBShell
=====
1. Run `pipenv run dbshell` to start up a dbshell
2. For programming usage, refer to `DBShell` class under `utils/dbshell.py`

Index Definition/Manipulation
=====
- Index definition example
```python
from iu_mongo.document import Document, EmbeddedDocument
from iu_mongo.fields import *
from iu_mongo.index import IndexDefinition

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
```
- Index manipulation example
```
In [1]: TestDoc.list_indexes()                                                                                                                                
test_int_1               ()        DEFINED                       COVERED        
_id_                     ()        DEFINED        BUILT                         
test_list_1              (TTL)     DEFINED                                      
test_pk_-1               (UNIQUE)  DEFINED                                      
test_dict_1              (SPARSE)  DEFINED                                      
test_pk_-1_test_int_1    ()        DEFINED                                      
test_pk_1_test_int_1     (UNIQUE)  DEFINED                                      
test_int_1_test_list_1   (UNIQUE)  DEFINED 

In [2]: TestDoc.create_indexes()                                                                                                                              
Will build index test_list_1_TTL, are you sure? (yes/no)yes
Index built in background, please check that after a while
Will build index test_pk_-1_UNIQUE, are you sure? (yes/no)yes
Index built in background, please check that after a while
Will build index test_dict_1_SPARSE, are you sure? (yes/no)yes
Index built in background, please check that after a while
Will build index test_pk_-1_test_int_1, are you sure? (yes/no)yes
Index built in background, please check that after a while
Will build index test_pk_1_test_int_1_UNIQUE, are you sure? (yes/no)yes
Index built in background, please check that after a while
Will build index test_int_1_test_list_1_UNIQUE, are you sure? (yes/no)yes
Index built in background, please check that after a while

In [13]: TestDoc.drop_index('test_pk_-1')
```

Contribute guidelines
=====
1. Make sure you have [pipenv](https://pipenv.readthedocs.io/en/latest/) and python3 environment
2. Clone or fork this repo then run `pipenv install` in the repo directory to setup a python virtual environment with all dev dependencies installed.
3. Write your codes to add new features to iu_mongo
4. Before commit your codes, please write unit test to make sure iu_mongo will perform well based on your change, run `pipenv run test_all` to issue an entire testing and make sure all test cases are PASSED. You can also run `pipenv run test_single MODULE/CLASS/CLASS METHOD/MODULE PATH` to just issue a test in your test module or class, this is useful when you just want your new-write test cases are tested.
5. After everything is done (write codes, test codes), push your commits and issue a pull request if needed be.

TODO
=====
1. Setup mongodb document validation rules in mongodb layer, not mongo driver layer