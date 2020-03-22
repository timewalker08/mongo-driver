import contextlib
import pymongo
import warnings
from bson import ObjectId
from mongo_driver.errors import BulkOperationError
from pymongo.write_concern import WriteConcern
from pymongo.operations import UpdateMany, UpdateOne, DeleteMany, DeleteOne, InsertOne
from mongo_driver.mixin.base import BaseMixin


class BulkContext(object):
    def __init__(self, pymongo_collection, ordered, session=None):
        self._ordered = ordered
        self._pymongo_collection = pymongo_collection
        self._requests = []
        self._pymongo_session = session and session.pymongo_session

    def bulk_update(self, filter, document, upsert, multi):
        if multi:
            self._requests.append(UpdateMany(filter, document, upsert=upsert))
        else:
            self._requests.append(UpdateOne(filter, document, upsert=upsert))

    def bulk_remove(self, filter, multi):
        if multi:
            self._requests.append(DeleteMany(filter))
        else:
            self._requests.append(DeleteOne(filter))

    def bulk_save(self, doc):
        self._requests.append(InsertOne(doc))

    def execute(self):
        if len(self._requests) == 0:
            return
        try:
            self._pymongo_result = self._pymongo_collection.bulk_write(
                self._requests, ordered=self._ordered, session=self._pymongo_session)
        except pymongo.errors.BulkWriteError as e:
            raise BulkOperationError(e)


class BulkMixin(BaseMixin):
    @classmethod
    @contextlib.contextmanager
    def bulk(cls, allow_empty=True, unordered=False, session=None):
        pymongo_collection = cls._pymongo()
        bulk_context = BulkContext(
            pymongo_collection, not unordered, session=session)
        yield bulk_context
        bulk_context.execute()

    @classmethod
    def bulk_update(cls, bulk_context, filter, document, upsert=False, multi=True):
        if not document:
            raise ValueError("Cannot do empty updates")
        document = cls._transform_value(document)
        if not filter:
            raise ValueError("Cannot do empty filters")
        filter = cls._update_filter(filter)
        bulk_context.bulk_update(filter, document, upsert, multi)

    @classmethod
    def bulk_remove(cls, bulk_context, filter, multi=True):
        if not filter:
            raise ValueError("Cannot do empty filters")
        filter = cls._update_filter(filter)
        bulk_context.bulk_remove(filter, multi)

    def bulk_save(self, bulk_context):
        cls = self.__class__
        self.validate()
        doc = self.to_mongo()
        bulk_context.bulk_save(doc)

    def bulk_update_one(self, bulk_context, document):
        self.bulk_update(bulk_context, {'id': self.id}, document, multi=False)

    def bulk_set(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$set': kwargs})

    def bulk_unset(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$unset': kwargs})

    def bulk_inc(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$inc': kwargs})

    def bulk_push(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$push': kwargs})

    def bulk_pull(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$pull': kwargs})

    def bulk_add_to_set(self, bulk_context, **kwargs):
        return self.bulk_update_one(bulk_context, {'$addToSet': kwargs})
