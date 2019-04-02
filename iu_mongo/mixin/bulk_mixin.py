import contextlib
import pymongo
import warnings
from bson import ObjectId
from iu_mongo.errors import OperationError
from pymongo.write_concern import WriteConcern
from iu_mongo.mixin.base import BaseMixin


class BulkOperationError(OperationError):
    pass


class BulkMixin(BaseMixin):
    @classmethod
    @contextlib.contextmanager
    def bulk(cls, allow_empty=True, unordered=False):
        w = cls._meta.get('write_concern')
        pymongo_collection = cls._pymongo(write_concern=WriteConcern(w=w))
        if unordered:
            bulk_context = pymongo_collection.initialize_unordered_bulk_op()
        else:
            bulk_context = pymongo_collection.initialize_ordered_bulk_op()
        yield bulk_context
        try:
            bulk_context.execute()
        except pymongo.errors.BulkWriteError as e:
            wc_errors = e.details.get('writeConcernErrors')
            # only one write error should occur for an ordered op
            w_error = e.details['writeErrors'][0] \
                if e.details.get('writeErrors') else None
            if wc_errors:
                messages = '\n'.join(_['errmsg'] for _ in wc_errors)
                message = 'Write concern errors for bulk op: %s' % messages
            elif w_error:
                message = 'Write errors for bulk op: %s' % \
                    w_error['errmsg']
            bo_error = BulkOperationError(message)
            bo_error.details = e.details
            if w_error:
                bo_error.op = w_error['op']
                bo_error.index = w_error['index']
            raise bo_error
        except pymongo.errors.InvalidOperation as e:
            if 'No operations' in str(e):
                if allow_empty is None:
                    warnings.warn('Empty bulk operation; use allow_empty')
                elif allow_empty is False:
                    raise
                else:
                    pass
            else:
                raise
        except pymongo.errors.OperationFailure as err:
            message = u'Could not perform bulk operation (%s)' % err.message
            raise OperationError(message)

    @classmethod
    def bulk_update(cls, bulk_context, filter, document, upsert=False, multi=True):
        if not document:
            raise ValueError("Cannot do empty updates")
        if not filter:
            raise ValueError("Cannot do empty filters")
        filter = cls._update_filter(filter)
        op = bulk_context.find(filter)
        if upsert:
            op = op.upsert()
        if multi:
            op.update(document)
        else:
            op.update_one(document)

    @classmethod
    def bulk_remove(cls, bulk_context, filter, multi=True):
        if not filter:
            raise ValueError("Cannot do empty filters")
        filter = cls._update_filter(filter)
        op = bulk_context.find(filter)
        if multi:
            op.remove()
        else:
            op.remove_one()

    def bulk_save(self, bulk_context):
        cls = self.__class__
        self.validate()
        doc = self.to_mongo()
        id_field = cls.id
        id_name = id_field.name or 'id'
        if self[id_name] is None:
            object_id = ObjectId()
            doc[id_field.db_field] = id_field.to_mongo(object_id)
        else:
            object_id = self[id_name]
        bulk_context.insert(doc)
        return object_id

    def bulk_update_one(self, bulk_context, document):
        self.bulk_update(bulk_context, {'id': self.id}, document)

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
