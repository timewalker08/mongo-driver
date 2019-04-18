import contextlib
import pymongo
import warnings
from pymongo.write_concern import WriteConcern
from bson import SON, ObjectId
from iu_mongo.errors import OperationError
from iu_mongo.mixin.base import BaseMixin
from iu_mongo.mixin.bulk_mixin import BulkMixin
from iu_mongo.timer import log_slow_event


class WriteMixin(BulkMixin, BaseMixin):
    @classmethod
    def drop_collection(cls):
        pymongo_collection = cls._pymongo(
            write_concern=WriteConcern(w=cls._meta['write_concern']))
        pymongo_collection.drop()

    @classmethod
    def update(cls, filter, document, upsert=False, multi=True):
        if not document:
            raise ValueError("Cannot do empty updates")
        document = cls._transform_value(document)
        if not filter:
            raise ValueError("Cannot do empty filters")
        filter = cls._update_filter(filter)
        with log_slow_event("update", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(
                write_concern=WriteConcern(w=cls._meta['write_concern']))
            if multi:
                result = pymongo_collection.update_many(
                    filter, document, upsert=upsert)
            else:
                result = pymongo_collection.update_one(
                    filter, document, upsert=upsert)
        result_dict = {
            'matched_count': result.matched_count,
            'modified_count': result.modified_count,
            'upserted_id': result.upserted_id,
        }
        result_dict.update(result.raw_result)
        return result_dict

    @classmethod
    def find_and_modify(cls, filter, update=None, sort=None, remove=False,
                        new=False, projection=None, upsert=False):
        if not update and not remove:
            raise ValueError("Cannot have empty update and no remove flag")
        # handle queries with inheritance
        filter = cls._update_filter(filter)
        update = cls._transform_value(update)
        from pymongo.collection import ReturnDocument
        with log_slow_event("find_and_modify", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(
                write_concern=WriteConcern(w=cls._meta['write_concern']))
            if remove:
                result = pymongo_collection.find_one_and_delete(
                    filter,
                    sort=sort,
                    projection=projection
                )
            else:
                result = pymongo_collection.find_one_and_update(
                    filter, update,
                    sort=sort,
                    projection=projection,
                    upsert=upsert,
                    return_document=ReturnDocument.AFTER if new else
                    ReturnDocument.BEFORE
                )
        if result:
            return cls._from_son(result)
        else:
            return None

    @classmethod
    def remove(cls, filter, multi=True):
        filter = cls._update_filter(filter)
        with log_slow_event("remove", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(
                write_concern=WriteConcern(w=cls._meta['write_concern']))
            if multi:
                result = pymongo_collection.delete_many(filter)
            else:
                result = pymongo_collection.delete_one(filter)
        result_dict = {
            'deleted_count': result.deleted_count,
        }
        result_dict.update(result.raw_result)
        return result_dict

    def save(self):
        cls = self.__class__
        force_insert = self._meta['force_insert']
        self.validate()
        doc = self.to_mongo()
        try:
            w = self._meta['write_concern']
            collection = self._pymongo(write_concern=WriteConcern(w=w))
            if force_insert or "_id" not in doc:
                pk_value = collection.insert_one(doc).inserted_id
            else:
                collection.replace_one({'_id': doc['_id']}, doc)
                pk_value = doc['_id']
        except pymongo.errors.OperationFailure as err:
            message = 'Could not save document (%s)'
            raise OperationError(message % err)
        self.id = cls.id.to_python(pk_value)
        return pk_value

    def delete(self):
        cls = self.__class__
        object_id = cls.id.to_mongo(self.id)
        try:
            self.remove({'id': object_id})
        except pymongo.errors.OperationFailure as err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

    def update_one(self, document):
        if not document:
            raise ValueError("Cannot do empty updates")
        document = self._transform_value(document)
        query_filter = self._update_one_key()
        with log_slow_event("update_one", self._meta['collection'], query_filter):
            result = self.find_and_modify(query_filter,
                                          update=document,
                                          new=True)
            if result:
                for field in self._fields:
                    setattr(self, field, result[field])
        return result

    def set(self, **kwargs):
        return self.update_one({'$set': kwargs})

    def unset(self, **kwargs):
        return self.update_one({'$unset': kwargs})

    def inc(self, **kwargs):
        return self.update_one({'$inc': kwargs})

    def push(self, **kwargs):
        return self.update_one({'$push': kwargs})

    def pull(self, **kwargs):
        return self.update_one({'$pull': kwargs})

    def add_to_set(self, **kwargs):
        return self.update_one({'$addToSet': kwargs})
