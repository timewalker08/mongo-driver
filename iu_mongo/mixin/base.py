import time
import pymongo
import logging
from pymongo.read_preferences import ReadPreference
from bson import SON, DBRef, ObjectId
from iu_mongo.base import BaseDocument, get_document
from iu_mongo.errors import ValidationError
from iu_mongo.timer import log_slow_event
from iu_mongo.connection import ConnectionError
from iu_mongo import SlaveOkSetting
from iu_mongo.utils.terminal import color_terminal, Color

RETRY_ERRORS = (
    pymongo.errors.ConnectionFailure,
    ConnectionError
)
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
    def _pymongo(cls, read_preference=None, write_concern=None):
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
        if not isinstance(filter, dict):
            return filter
        # handle queries with inheritance
        if cls._meta.get('allow_inheritance'):
            filter['_types'] = cls._class_name
        if 'id' in filter:
            filter['_id'] = filter['id']
            del filter['id']
        return filter

    @classmethod
    def list_indexes(cls, display=True):
        from iu_mongo import TaggedIndex, IndexDefinition
        from copy import copy
        desired_indexes = set([TaggedIndex.parse_from_index_def(index_def)
                               for index_def in cls._meta['indexes']])
        desired_indexes.add(
            TaggedIndex.parse_from_index_def(
                IndexDefinition.parse_from_keys_str('_id:1'),
            )
        )
        pymongo_collection = cls._pymongo(
            read_preference=SlaveOkSetting.TO_PYMONGO[SlaveOkSetting.PRIMARY])
        pymongo_indexes = pymongo_collection.index_information()
        exist_indexes = set([
            TaggedIndex.parse_from_pymongo_index_def(index_name, index_def)
            for index_name, index_def in pymongo_indexes.items()
        ])
        all_indexes = []
        final_indexes = []
        for i1 in desired_indexes:
            for i2 in exist_indexes:
                if i1 == i2:
                    i1.tag_property = i2.tag_property = (
                        i1.tag_property ^ i2.tag_property)
                    i1.real_name = i2.real_name
            all_indexes.append(copy(i1))
        exist_indexes -= desired_indexes
        all_indexes.extend(list(exist_indexes))
        # find covered indexes
        for i1 in all_indexes:
            # consider normal index only
            if i1.index_property == 0:
                for i2 in all_indexes:
                    if i1 == i2:
                        continue
                    # ttl index will not cover any indexes, sparse index not considered now
                    if i2.sparse or i2.ttl:
                        continue
                    if i1.is_covered_by(i2):
                        i1.tag_property ^= TaggedIndex.TagProperty.COVERED
            final_indexes.append(copy(i1))
        final_indexes = sorted(
            final_indexes, key=lambda index: index.tag_property, reverse=True)
        if not display:
            return final_indexes
        else:
            pre = None
            for index in final_indexes:
                color = None
                if index.covered:
                    color = Color.FAIL
                elif index.built and index.defined:
                    color = Color.OKGREEN
                elif index.defined:
                    color = Color.OKBLUE
                else:
                    color = Color.FAIL
                with color_terminal(color) as out:
                    out('%-25s%-10s' % (index.real_name or index.name, index.properties_str)+'%-15s%-15s%-15s' % (
                        'DEFINED' if index.defined else '',
                        'BUILT' if index.built else '',
                        'COVERED' if index.covered else ''
                    ))

    @classmethod
    def create_indexes(cls):
        pymongo_collection = cls._pymongo()
        all_indexes = cls.list_indexes(display=False)
        for index in all_indexes:
            if index.built or index.covered or not index.defined:
                continue
            if input("Will build index %s, are you sure? (yes/no)" % str(index)) != 'yes':
                continue
            extra_opts = {
            }
            if index.expire_after_seconds is not None:
                extra_opts['expireAfterSeconds'] = index.expire_after_seconds
            pymongo_collection.create_index(
                index.to_pymongo_keys(),
                background=True,
                unique=index.unique,
                sparse=index.sparse, **extra_opts)
            print('Index built in background, please check that after a while')

    @classmethod
    def drop_index(cls, index_name):
        pymongo_collection = cls._pymongo()
        pymongo_collection.drop_index(index_name)
