import contextlib
import pymongo
import warnings
from pymongo.write_concern import WriteConcern
from bson import SON, ObjectId
from mongo_driver.errors import OperationError
from mongo_driver.mixin.base import BaseMixin
from mongo_driver.mixin.bulk_mixin import BulkMixin
from mongo_driver.timer import log_slow_event
from mongo_driver.session import Session


class WriteMixin(BulkMixin, BaseMixin):
    @classmethod
    def drop_collection(cls):
        pymongo_collection = cls._pymongo()
        pymongo_collection.drop()

    @classmethod
    def update(cls, filter, document, upsert=False, multi=True, session=None):
        document = cls._transform_value(document)
        filter = cls._update_filter(filter)
        with log_slow_event("update", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo()
            if multi:
                result = pymongo_collection.update_many(
                    filter, document, upsert=upsert,
                    session=session and session.pymongo_session)
            else:
                result = pymongo_collection.update_one(
                    filter, document, upsert=upsert,
                    session=session and session.pymongo_session)
        result_dict = {
            'matched_count': result.matched_count,
            'modified_count': result.modified_count,
            'upserted_id': result.upserted_id,
        }
        result_dict.update(result.raw_result)
        return result_dict

    @classmethod
    def find_and_modify(cls, filter, update=None, sort=None, remove=False,
                        new=False, projection=None, upsert=False, session=None):
        if not update and not remove:
            raise ValueError("Cannot have empty update and no remove flag")
        # handle queries with inheritance
        filter = cls._update_filter(filter)
        update = cls._transform_value(update)
        from pymongo.collection import ReturnDocument
        with log_slow_event("find_and_modify", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo()
            if remove:
                result = pymongo_collection.find_one_and_delete(
                    filter,
                    sort=sort,
                    projection=projection,
                    session=session and session.pymongo_session
                )
            else:
                result = pymongo_collection.find_one_and_update(
                    filter, update,
                    sort=sort,
                    projection=projection,
                    upsert=upsert,
                    return_document=ReturnDocument.AFTER if new else
                    ReturnDocument.BEFORE,
                    session=session and session.pymongo_session
                )
        if result:
            return cls._from_son(result)
        else:
            return None

    @classmethod
    def remove(cls, filter, multi=True, session=None):
        filter = cls._update_filter(filter)
        with log_slow_event("remove", cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo()
            if multi:
                result = pymongo_collection.delete_many(
                    filter, session=session and session.pymongo_session)
            else:
                result = pymongo_collection.delete_one(
                    filter, session=session and session.pymongo_session)
        result_dict = {
            'deleted_count': result.deleted_count,
        }
        result_dict.update(result.raw_result)
        return result_dict

    def save(self, session=None):
        cls = self.__class__
        force_insert = self._meta['force_insert']
        self.validate()
        doc = self.to_mongo()
        try:
            collection = self._pymongo()
            if force_insert or "_id" not in doc:
                pk_value = collection.insert_one(doc,
                                                 session=session and session.pymongo_session).inserted_id
            else:
                collection.replace_one(
                    {'_id': doc['_id']}, doc, session=session and session.pymongo_session)
                pk_value = doc['_id']
        except pymongo.errors.OperationFailure as err:
            message = 'Could not save document (%s)'
            raise OperationError(message % err)
        self.id = cls.id.to_python(pk_value)
        return pk_value

    def delete(self, session=None):
        cls = self.__class__
        object_id = cls.id.to_mongo(self.id)
        try:
            self.remove({'id': object_id}, session=session)
        except pymongo.errors.OperationFailure as err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

    def update_one(self, document, session=None):
        document = self._transform_value(document)
        query_filter = self._update_one_key()
        with log_slow_event("update_one", self._meta['collection'], query_filter):
            result = self.find_and_modify(query_filter,
                                          update=document,
                                          new=True, session=session)
            if result:
                for field in self._fields:
                    setattr(self, field, result[field])
        return result

    def set(self, _session=None, **kwargs):
        return self.update_one({'$set': kwargs}, session=_session)

    def unset(self, _session=None, **kwargs):
        return self.update_one({'$unset': kwargs}, session=_session)

    def inc(self, _session=None, **kwargs):
        return self.update_one({'$inc': kwargs}, session=_session)

    def push(self, _session=None, **kwargs):
        return self.update_one({'$push': kwargs}, session=_session)

    def pull(self, _session=None, **kwargs):
        return self.update_one({'$pull': kwargs}, session=_session)

    def add_to_set(self, _session=None, **kwargs):
        return self.update_one({'$addToSet': kwargs}, session=_session)
