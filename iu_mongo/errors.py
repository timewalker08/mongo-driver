from collections import defaultdict

import six
from six import iteritems

__all__ = ('NotRegistered', 'InvalidDocumentError', 'LookUpError',
           'DoesNotExist', 'MultipleObjectsReturned', 'InvalidQueryError',
           'OperationError', 'NotUniqueError', 'FieldDoesNotExist',
           'ValidationError', 'SaveConditionError')


class IUMongoError(Exception):
    pass


class NotRegistered(IUMongoError):
    pass


class InvalidDocumentError(IUMongoError):
    pass


class LookUpError(AttributeError):
    pass


class DoesNotExist(IUMongoError):
    pass


class MultipleObjectsReturned(IUMongoError):
    pass


class InvalidQueryError(IUMongoError):
    pass


class OperationError(IUMongoError):
    pass


class NotUniqueError(OperationError):
    pass


class SaveConditionError(OperationError):
    pass


class FieldDoesNotExist(IUMongoError):
    pass


class ConnectionError(IUMongoError):
    pass


class BulkOperationError(OperationError):
    def __init__(self, pymongo_bulk_write_error):
        self._pymongo_error = pymongo_bulk_write_error
        super(BulkOperationError, self).__init__('Bulk Write Error')


class ValidationError(AssertionError):
    """Validation exception.

    May represent an error validating a field or a
    document containing fields with validation errors.

    :ivar errors: A dictionary of errors for fields within this
        document or list, or None if the error is for an
        individual field.
    """

    errors = {}
    field_name = None
    _message = None

    def __init__(self, message='', **kwargs):
        super(ValidationError, self).__init__(message)
        self.errors = kwargs.get('errors', {})
        self.field_name = kwargs.get('field_name')
        self.message = message

    def __str__(self):
        return six.text_type(self.message)

    def __repr__(self):
        return '%s(%s,)' % (self.__class__.__name__, self.message)

    def __getattribute__(self, name):
        message = super(ValidationError, self).__getattribute__(name)
        if name == 'message':
            if self.field_name:
                message = '%s' % message
            if self.errors:
                message = '%s(%s)' % (message, self._format_errors())
        return message

    def _get_message(self):
        return self._message

    def _set_message(self, message):
        self._message = message

    message = property(_get_message, _set_message)

    def to_dict(self):
        """Returns a dictionary of all errors within a document

        Keys are field names or list indices and values are the
        validation error messages, or a nested dictionary of
        errors for an embedded document or list.
        """

        def build_dict(source):
            errors_dict = {}
            if not source:
                return errors_dict

            if isinstance(source, dict):
                for field_name, error in iteritems(source):
                    errors_dict[field_name] = build_dict(error)
            elif isinstance(source, ValidationError) and source.errors:
                return build_dict(source.errors)
            else:
                return six.text_type(source)

            return errors_dict

        if not self.errors:
            return {}

        return build_dict(self.errors)

    def _format_errors(self):
        """Returns a string listing all errors within a document"""

        def generate_key(value, prefix=''):
            if isinstance(value, list):
                value = ' '.join([generate_key(k) for k in value])
            elif isinstance(value, dict):
                value = ' '.join(
                    [generate_key(v, k) for k, v in iteritems(value)])

            results = '%s.%s' % (prefix, value) if prefix else value
            return results

        error_dict = defaultdict(list)
        for k, v in iteritems(self.to_dict()):
            error_dict[generate_key(v)].append(k)
        return ' '.join(['%s: %s' % (k, v) for k, v in iteritems(error_dict)])
