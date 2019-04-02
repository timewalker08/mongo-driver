import time
import pymongo
import logging
from pymongo.read_preferences import ReadPreference
from bson import SON, DBRef, ObjectId
from iu_mongo.base import BaseDocument, get_document
from iu_mongo.errors import ValidationError
from iu_mongo.timer import log_slow_event
from iu_mongo.connection import ConnectionError

RETRY_ERRORS = (pymongo.errors.PyMongoError, ConnectionError)
RETRY_LOGGER = logging.getLogger('iu_mongo.pymongo_retry')


class BaseMixin(object):
    @classmethod
    def _check_read_max_time_ms(cls, action_name, max_time_ms, read_preference):
        if (not (max_time_ms > 0 and max_time_ms < 10000)) and \
            (read_preference == ReadPreference.PRIMARY or
                read_preference == ReadPreference.PRIMARY_PREFERRED):
            logger = logging.getLogger('iu_mongo.document.max_time_ms')
            logger.warn(
                'Collection %s: no timeout or large timeout for %s operation on primary node',
                cls.__name__, action_name)

    def _update_one_key(self):
        key = {'_id': self.id}
        return key

    @classmethod
    def _by_id_key(cls, doc_id):
        key = {'_id': doc_id}
        return key

    @classmethod
    def _by_ids_key(cls, doc_ids):
        key = {'_id': {'$in': doc_ids}}
        return key

    @classmethod
    def _pymongo(cls, use_async=True, read_preference=None, write_concern=None):
        from iu_mongo.connection import _get_db
        database = None
        collection = None
        database = _get_db(cls._meta['db_name'])
        if database:
            collection = database[cls._meta['collection']]
        if not collection or not database:
            raise ConnectionError(
                'No mongo connections for collection %s' % cls.__name__)
        return collection.with_options(
            read_preference=read_preference,
            write_concern=write_concern)

    @classmethod
    def _update_filter(cls, filter):
        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            filter['_types'] = cls._class_name
        if 'id' in filter:
            filter['_id'] = filter['id']
            del filter['id']
        return filter
