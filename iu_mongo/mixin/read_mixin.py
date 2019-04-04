import logging
import traceback
import pymongo
import time
from retry import retry
from bson import ObjectId
from pymongo.read_preferences import ReadPreference
from iu_mongo.mixin.base import BaseMixin, RETRY_ERRORS,\
    RETRY_LOGGER
from iu_mongo.base import SlaveOkSetting
from iu_mongo.timer import log_slow_event


class ReadMixin(BaseMixin):
    MAX_TIME_MS = 5000
    FIND_WARNING_DOCS_LIMIT = 10000

    @classmethod
    def _count(cls, slave_ok=SlaveOkSetting.PRIMARY, filter={},
               hint=None, limit=None, skip=0, max_time_ms=None):
        filter = cls._update_filter(filter)
        read_preference = SlaveOkSetting.TO_PYMONGO[slave_ok]
        pymongo_collection = cls._pymongo(read_preference=read_preference)
        max_time_ms = max_time_ms or cls.MAX_TIME_MS
        cls._check_read_max_time_ms(
            'count_documents', max_time_ms, pymongo_collection.read_preference)
        with log_slow_event('count_documents', cls._meta['collection'], filter):
            kwargs_dict = {
                'skip': skip,
            }
            if hint:
                kwargs_dict.update({
                    'hint': hint
                })
            if limit > 0:
                kwargs_dict.update({
                    'limit': limit
                })
            if max_time_ms > 0:
                kwargs_dict.update({
                    'maxTimeMS': max_time_ms
                })
            return pymongo_collection.count_documents(filter, **kwargs_dict)

    @classmethod
    def _find_raw(cls, filter, projection=None, skip=0, limit=0, sort=None,
                  slave_ok=SlaveOkSetting.PRIMARY, find_one=False, hint=None,
                  batch_size=10000, max_time_ms=None):
        # transform query
        filter = cls._update_filter(filter)

        read_preference = SlaveOkSetting.TO_PYMONGO[slave_ok]

        with log_slow_event('find', cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(
                read_preference=read_preference)
            cur = pymongo_collection.find(filter, projection,
                                          skip=skip, limit=limit,
                                          sort=sort)

            max_time_ms = max_time_ms or cls.MAX_TIME_MS
            cls._check_read_max_time_ms(
                'find', max_time_ms, pymongo_collection.read_preference)

            if max_time_ms > 0:
                cur.max_time_ms(max_time_ms)

            if hint:
                cur.hint(hint)

            if find_one:
                for result in cur.limit(-1):
                    return result
                return None
            else:
                cur.batch_size(batch_size)

            return cur

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find(cls, filter, projection=None, skip=0, limit=0, sort=None,
             slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None):
        cur = cls._find_raw(filter, projection=projection, skip=skip,
                            limit=limit, sort=sort,
                            slave_ok=slave_ok,
                            max_time_ms=max_time_ms)
        results = []
        total = 0
        for doc in cur:
            total += 1
            results.append(cls._from_son(doc))
            if total == cls.FIND_WARNING_DOCS_LIMIT + 1:
                logging.getLogger('iu_mongo.read.find_warning').warn(
                    'Collection %s: return more than %d docs in one FIND action, '
                    'consider to use FIND_ITER.',
                    cls.__name__,
                    cls.FIND_WARNING_DOCS_LIMIT,
                )
        return results

    @classmethod
    def find_iter(cls, filter, projection=None, skip=0, limit=0, sort=None,
                  slave_ok=SlaveOkSetting.PRIMARY, batch_size=10000, max_time_ms=None):
        cur = cls._find_raw(filter, projection=projection, skip=skip,
                            limit=limit, sort=sort, slave_ok=slave_ok,
                            batch_size=batch_size,
                            max_time_ms=max_time_ms)
        last_doc = None
        for doc in cur:
            last_doc = cls._from_son(doc)
            yield last_doc

    @classmethod
    def aggregate(cls, pipeline=None, slave_ok=SlaveOkSetting.OFFLINE):
        # TODO max_time_ms: timeout control needed
        read_preference = SlaveOkSetting.TO_PYMONGO[slave_ok]
        pymongo_collection = cls._pymongo(read_preference=read_preference)
        cursor_iter = pymongo_collection.aggregate(pipeline)
        for doc in cursor_iter:
            yield cls._from_son(doc)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def distinct(cls, filter, key, skip=0, limit=0, sort=None,
                 slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None):
        cur = cls._find_raw(filter, skip=skip, limit=limit,
                            sort=sort, slave_ok=slave_ok,
                            max_time_ms=max_time_ms)
        return cur.distinct(key)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find_one(cls, filter, projection=None, sort=None, slave_ok=SlaveOkSetting.PRIMARY,
                 max_time_ms=None):
        doc = cls._find_raw(filter, projection=projection, sort=sort,
                            slave_ok=slave_ok, find_one=True,
                            max_time_ms=max_time_ms)
        if doc:
            return cls._from_son(doc)
        else:
            return None

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def count(cls, filter={}, slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None,
              skip=0, limit=0, hint=None):
        return cls._count(filter=filter, slave_ok=slave_ok,
                          max_time_ms=max_time_ms,
                          hint=hint,
                          skip=skip, limit=limit)

    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def reload(self, slave_ok=SlaveOkSetting.PRIMARY):
        obj = self.__class__.find_one(self._by_id_key(self.id),
                                      slave_ok=slave_ok)
        for field in self._fields:
            setattr(self, field, obj[field])

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def by_id(cls, doc_id, **kwargs):
        if isinstance(doc_id, str):
            doc_id = ObjectId(doc_id)
        return cls.find_one(cls._by_id_key(doc_id), **kwargs)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def by_ids(cls, doc_ids, **kwargs):
        new_doc_ids = [ObjectId(doc_id) for doc_id in doc_ids]
        return cls.find(cls._by_ids_key(new_doc_ids), **kwargs)
