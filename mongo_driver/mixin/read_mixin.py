import logging
import traceback
import pymongo
import time
from retry import retry
from bson import ObjectId
from pymongo.read_preferences import ReadPreference
from mongo_driver.mixin.base import BaseMixin, RETRY_ERRORS,\
    RETRY_LOGGER
from mongo_driver.timer import log_slow_event
from mongo_driver import SlaveOkSetting


class ReadMixin(BaseMixin):
    MAX_TIME_MS = 5000
    FIND_WARNING_DOCS_LIMIT = 10000

    @classmethod
    def _count(cls, slave_ok=SlaveOkSetting.PRIMARY, filter={},
               hint=None, limit=None, skip=0, max_time_ms=None, session=None):
        filter = cls._update_filter(filter)
        pymongo_collection = cls._pymongo(slave_ok_setting=slave_ok)
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
            if session:
                kwargs_dict.update({
                    'session': session.pymongo_session
                })
            return pymongo_collection.count_documents(filter, **kwargs_dict)

    @classmethod
    def _find_raw(cls, filter, projection=None, skip=0, limit=0, sort=None,
                  slave_ok=SlaveOkSetting.PRIMARY, find_one=False, hint=None,
                  batch_size=10000, max_time_ms=None, session=None):
        # transform query
        filter = cls._update_filter(filter)
        with log_slow_event('find', cls._meta['collection'], filter):
            pymongo_collection = cls._pymongo(slave_ok_setting=slave_ok)
            cur = pymongo_collection.find(filter, projection,
                                          skip=skip, limit=limit,
                                          sort=sort,
                                          session=session and session.pymongo_session)

            max_time_ms = max_time_ms or cls.MAX_TIME_MS
            cls._check_read_max_time_ms(
                'find', max_time_ms, pymongo_collection.read_preference)

            if max_time_ms > 0:
                cur.max_time_ms(max_time_ms)

            if hint:
                cur.hint(hint)

            if find_one:
                for result in cur.limit(1):
                    return result
                return None
            else:
                cur.batch_size(batch_size)

            return cur

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find(cls, filter, projection=None, skip=0, limit=0, sort=None,
             slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None, session=None):
        cur = cls._find_raw(filter, projection=projection, skip=skip,
                            limit=limit, sort=sort,
                            slave_ok=slave_ok,
                            max_time_ms=max_time_ms, session=session)
        results = []
        total = 0
        for doc in cur:
            total += 1
            results.append(cls._from_son(doc))
            if total == cls.FIND_WARNING_DOCS_LIMIT + 1:
                logging.getLogger('mongo_driver.read.find_warning').warn(
                    'Collection %s: return more than %d docs in one FIND action, '
                    'consider to use FIND_ITER.',
                    cls.__name__,
                    cls.FIND_WARNING_DOCS_LIMIT,
                )
        return results

    @classmethod
    def find_iter(cls, filter, projection=None, skip=0, limit=0, sort=None,
                  slave_ok=SlaveOkSetting.PRIMARY, batch_size=10000, max_time_ms=None,
                  session=None):
        cur = cls._find_raw(filter, projection=projection, skip=skip,
                            limit=limit, sort=sort, slave_ok=slave_ok,
                            batch_size=batch_size, max_time_ms=max_time_ms,
                            session=session)
        last_doc = None
        for doc in cur:
            last_doc = cls._from_son(doc)
            yield last_doc

    @classmethod
    def aggregate(cls, pipeline=None, slave_ok=SlaveOkSetting.OFFLINE,
                  session=None):
        # TODO max_time_ms: timeout control needed
        read_preference = SlaveOkSetting.TO_PYMONGO[slave_ok]
        pymongo_collection = cls._pymongo(slave_ok_setting=slave_ok)
        cursor_iter = pymongo_collection.aggregate(pipeline,
                                                   session=session and session.pymongo_session)
        for doc in cursor_iter:
            yield doc

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def distinct(cls, filter, key, skip=0, limit=0, sort=None,
                 slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None, session=None):
        cur = cls._find_raw(filter, skip=skip, limit=limit,
                            sort=sort, slave_ok=slave_ok,
                            max_time_ms=max_time_ms, session=session)
        return cur.distinct(key)

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def find_one(cls, filter, projection=None, sort=None, slave_ok=SlaveOkSetting.PRIMARY,
                 max_time_ms=None, session=None):
        doc = cls._find_raw(filter, projection=projection, sort=sort,
                            slave_ok=slave_ok, find_one=True,
                            max_time_ms=max_time_ms, session=session)
        if doc:
            return cls._from_son(doc)
        else:
            return None

    @classmethod
    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def count(cls, filter={}, slave_ok=SlaveOkSetting.PRIMARY, max_time_ms=None,
              skip=0, limit=0, hint=None, session=None):
        return cls._count(filter=filter, slave_ok=slave_ok,
                          max_time_ms=max_time_ms,
                          hint=hint, skip=skip, limit=limit, session=session)

    @retry(exceptions=RETRY_ERRORS, tries=5, delay=5, logger=RETRY_LOGGER)
    def reload(self, slave_ok=SlaveOkSetting.PRIMARY, session=None):
        obj = self.__class__.find_one(self._by_id_key(self.id),
                                      slave_ok=slave_ok, session=session)
        if obj:
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
