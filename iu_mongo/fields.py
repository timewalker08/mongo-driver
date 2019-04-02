import datetime
import decimal
import itertools
import re
import socket
import time
import uuid
from operator import itemgetter

from bson import Binary, ObjectId, SON
import pymongo
import six
from six import iteritems

try:
    import dateutil
except ImportError:
    dateutil = None
else:
    import dateutil.parser

try:
    from bson.int64 import Int64
except ImportError:
    Int64 = long


from iu_mongo.base import (BaseDocument, BaseField, ComplexBaseField, ObjectIdField,
                           get_document)
from iu_mongo.base.utils import LazyRegexCompiler
from iu_mongo.common import _import_class
from iu_mongo.document import Document, EmbeddedDocument
from iu_mongo.errors import InvalidQueryError, ValidationError


if six.PY3:
    # Useless as long as 2to3 gets executed
    # as it turns `long` into `int` blindly
    long = int


__all__ = (
    'StringField', 'URLField', 'EmailField', 'IntField', 'LongField',
    'FloatField', 'DecimalField', 'BooleanField', 'DateTimeField', 'DateField',
    'EmbeddedDocumentField', 'ObjectIdField', 'ListField',
    'SortedListField', 'EmbeddedDocumentListField', 'DictField', 'MapField',
    'BinaryField', 'UUIDField'
)

RECURSIVE_REFERENCE_CONSTANT = 'self'


class StringField(BaseField):
    """A unicode string field."""

    def __init__(self, regex=None, max_length=None, min_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.max_length = max_length
        self.min_length = min_length
        super(StringField, self).__init__(**kwargs)

    def to_python(self, value):
        if isinstance(value, six.text_type):
            return value
        try:
            value = value.decode('utf-8')
        except Exception:
            pass
        return value

    def validate(self, value):
        if not isinstance(value, six.string_types):
            self.error('StringField only accepts string values')

        if self.max_length is not None and len(value) > self.max_length:
            self.error('String value is too long')

        if self.min_length is not None and len(value) < self.min_length:
            self.error('String value is too short')

        if self.regex is not None and self.regex.match(value) is None:
            self.error('String value did not match validation regex')

    def lookup_member(self, member_name):
        return None

    def prepare_query_value(self, op, value):
        if not isinstance(op, six.string_types):
            return value

        if op.lstrip('i') in ('startswith', 'endswith', 'contains', 'exact'):
            flags = 0
            if op.startswith('i'):
                flags = re.IGNORECASE
                op = op.lstrip('i')

            regex = r'%s'
            if op == 'startswith':
                regex = r'^%s'
            elif op == 'endswith':
                regex = r'%s$'
            elif op == 'exact':
                regex = r'^%s$'

            # escape unsafe characters which could lead to a re.error
            value = re.escape(value)
            value = re.compile(regex % value, flags)
        return super(StringField, self).prepare_query_value(op, value)


class URLField(StringField):
    """A field that validates input as an URL.
    """

    _URL_REGEX = LazyRegexCompiler(
        r'^(?:[a-z0-9\.\-]*)://'  # scheme is validated separately
        # domain...
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-_]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}(?<!-)\.?)|'
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ...or ipv4
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ...or ipv6
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    _URL_SCHEMES = ['http', 'https', 'ftp', 'ftps']

    def __init__(self, url_regex=None, schemes=None, **kwargs):
        self.url_regex = url_regex or self._URL_REGEX
        self.schemes = schemes or self._URL_SCHEMES
        super(URLField, self).__init__(**kwargs)

    def validate(self, value):
        # Check first if the scheme is valid
        scheme = value.split('://')[0].lower()
        if scheme not in self.schemes:
            self.error(u'Invalid scheme {} in URL: {}'.format(scheme, value))
            return

        # Then check full URL
        if not self.url_regex.match(value):
            self.error(u'Invalid URL: {}'.format(value))
            return


class EmailField(StringField):
    """A field that validates input as an email address.
    """
    USER_REGEX = LazyRegexCompiler(
        # `dot-atom` defined in RFC 5322 Section 3.2.3.
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*\Z"
        # `quoted-string` defined in RFC 5322 Section 3.2.4.
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"\Z)',
        re.IGNORECASE
    )

    UTF8_USER_REGEX = LazyRegexCompiler(
        six.u(
            # RFC 6531 Section 3.3 extends `atext` (used by dot-atom) to
            # include `UTF8-non-ascii`.
            r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z\u0080-\U0010FFFF]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z\u0080-\U0010FFFF]+)*\Z"
            # `quoted-string`
            r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"\Z)'
        ), re.IGNORECASE | re.UNICODE
    )

    DOMAIN_REGEX = LazyRegexCompiler(
        r'((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+)(?:[A-Z0-9-]{2,63}(?<!-))\Z',
        re.IGNORECASE
    )

    error_msg = u'Invalid email address: %s'

    def __init__(self, domain_whitelist=None, allow_utf8_user=False,
                 allow_ip_domain=False, *args, **kwargs):
        """Initialize the EmailField.

        Args:
            domain_whitelist (list) - list of otherwise invalid domain
                                      names which you'd like to support.
            allow_utf8_user (bool) - if True, the user part of the email
                                     address can contain UTF8 characters.
                                     False by default.
            allow_ip_domain (bool) - if True, the domain part of the email
                                     can be a valid IPv4 or IPv6 address.
        """
        self.domain_whitelist = domain_whitelist or []
        self.allow_utf8_user = allow_utf8_user
        self.allow_ip_domain = allow_ip_domain
        super(EmailField, self).__init__(*args, **kwargs)

    def validate_user_part(self, user_part):
        """Validate the user part of the email address. Return True if
        valid and False otherwise.
        """
        if self.allow_utf8_user:
            return self.UTF8_USER_REGEX.match(user_part)
        return self.USER_REGEX.match(user_part)

    def validate_domain_part(self, domain_part):
        """Validate the domain part of the email address. Return True if
        valid and False otherwise.
        """
        # Skip domain validation if it's in the whitelist.
        if domain_part in self.domain_whitelist:
            return True

        if self.DOMAIN_REGEX.match(domain_part):
            return True

        # Validate IPv4/IPv6, e.g. user@[192.168.0.1]
        if (
            self.allow_ip_domain and
            domain_part[0] == '[' and
            domain_part[-1] == ']'
        ):
            for addr_family in (socket.AF_INET, socket.AF_INET6):
                try:
                    socket.inet_pton(addr_family, domain_part[1:-1])
                    return True
                except (socket.error, UnicodeEncodeError):
                    pass

        return False

    def validate(self, value):
        super(EmailField, self).validate(value)

        if '@' not in value:
            self.error(self.error_msg % value)

        user_part, domain_part = value.rsplit('@', 1)

        # Validate the user part.
        if not self.validate_user_part(user_part):
            self.error(self.error_msg % value)

        # Validate the domain and, if invalid, see if it's IDN-encoded.
        if not self.validate_domain_part(domain_part):
            try:
                domain_part = domain_part.encode('idna').decode('ascii')
            except UnicodeError:
                self.error(self.error_msg % value)
            else:
                if not self.validate_domain_part(domain_part):
                    self.error(self.error_msg % value)


class IntField(BaseField):
    """32-bit integer field."""

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(IntField, self).__init__(**kwargs)

    def to_python(self, value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            pass
        return value

    def validate(self, value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            self.error('%s could not be converted to int' % value)

        if self.min_value is not None and value < self.min_value:
            self.error('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Integer value is too large')

    def prepare_query_value(self, op, value):
        if value is None:
            return value

        return super(IntField, self).prepare_query_value(op, int(value))


class LongField(BaseField):
    """64-bit integer field."""

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(LongField, self).__init__(**kwargs)

    def to_python(self, value):
        try:
            value = long(value)
        except (TypeError, ValueError):
            pass
        return value

    def to_mongo(self, value):
        return Int64(value)

    def validate(self, value):
        try:
            value = long(value)
        except (TypeError, ValueError):
            self.error('%s could not be converted to long' % value)

        if self.min_value is not None and value < self.min_value:
            self.error('Long value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Long value is too large')

    def prepare_query_value(self, op, value):
        if value is None:
            return value

        return super(LongField, self).prepare_query_value(op, long(value))


class FloatField(BaseField):
    """Floating point number field."""

    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value, self.max_value = min_value, max_value
        super(FloatField, self).__init__(**kwargs)

    def to_python(self, value):
        try:
            value = float(value)
        except ValueError:
            pass
        return value

    def validate(self, value):
        if isinstance(value, six.integer_types):
            try:
                value = float(value)
            except OverflowError:
                self.error('The value is too large to be converted to float')

        if not isinstance(value, float):
            self.error('FloatField only accepts float and integer values')

        if self.min_value is not None and value < self.min_value:
            self.error('Float value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Float value is too large')

    def prepare_query_value(self, op, value):
        if value is None:
            return value

        return super(FloatField, self).prepare_query_value(op, float(value))


class DecimalField(BaseField):
    """Fixed-point decimal number field. Stores the value as a float by default unless `force_string` is used.
    If using floats, beware of Decimal to float conversion (potential precision loss)
    """

    def __init__(self, min_value=None, max_value=None, force_string=False,
                 precision=2, rounding=decimal.ROUND_HALF_UP, **kwargs):
        """
        :param min_value: Validation rule for the minimum acceptable value.
        :param max_value: Validation rule for the maximum acceptable value.
        :param force_string: Store the value as a string (instead of a float).
         Be aware that this affects query sorting and operation like lte, gte (as string comparison is applied)
         and some query operator won't work (e.g: inc, dec)
        :param precision: Number of decimal places to store.
        :param rounding: The rounding rule from the python decimal library:

            - decimal.ROUND_CEILING (towards Infinity)
            - decimal.ROUND_DOWN (towards zero)
            - decimal.ROUND_FLOOR (towards -Infinity)
            - decimal.ROUND_HALF_DOWN (to nearest with ties going towards zero)
            - decimal.ROUND_HALF_EVEN (to nearest with ties going to nearest even integer)
            - decimal.ROUND_HALF_UP (to nearest with ties going away from zero)
            - decimal.ROUND_UP (away from zero)
            - decimal.ROUND_05UP (away from zero if last digit after rounding towards zero would have been 0 or 5; otherwise towards zero)

            Defaults to: ``decimal.ROUND_HALF_UP``

        """
        self.min_value = min_value
        self.max_value = max_value
        self.force_string = force_string
        self.precision = precision
        self.rounding = rounding

        super(DecimalField, self).__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return value

        # Convert to string for python 2.6 before casting to Decimal
        try:
            value = decimal.Decimal('%s' % value)
        except (TypeError, ValueError, decimal.InvalidOperation):
            return value
        return value.quantize(decimal.Decimal('.%s' % ('0' * self.precision)), rounding=self.rounding)

    def to_mongo(self, value):
        if value is None:
            return value
        if self.force_string:
            return six.text_type(self.to_python(value))
        return float(self.to_python(value))

    def validate(self, value):
        if not isinstance(value, decimal.Decimal):
            if not isinstance(value, six.string_types):
                value = six.text_type(value)
            try:
                value = decimal.Decimal(value)
            except (TypeError, ValueError, decimal.InvalidOperation) as exc:
                self.error('Could not convert value to decimal: %s' % exc)

        if self.min_value is not None and value < self.min_value:
            self.error('Decimal value is too small')

        if self.max_value is not None and value > self.max_value:
            self.error('Decimal value is too large')

    def prepare_query_value(self, op, value):
        return super(DecimalField, self).prepare_query_value(op, self.to_mongo(value))


class BooleanField(BaseField):
    """Boolean field type.
    """

    def to_python(self, value):
        try:
            value = bool(value)
        except ValueError:
            pass
        return value

    def validate(self, value):
        if not isinstance(value, bool):
            self.error('BooleanField only accepts boolean values')


class DateTimeField(BaseField):
    """Datetime field.

    Uses the python-dateutil library if available alternatively use time.strptime
    to parse the dates.  Note: python-dateutil's parser is fully featured and when
    installed you can utilise it to convert varying types of date formats into valid
    python datetime objects.

    Note: To default the field to the current datetime, use: DateTimeField(default=datetime.utcnow)
    """

    def validate(self, value):
        new_value = self.to_mongo(value)
        if not isinstance(new_value, (datetime.datetime, datetime.date)):
            self.error(u'cannot parse date "%s"' % value)

    def to_mongo(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if callable(value):
            return value()

        if not isinstance(value, six.string_types):
            return None

        value = value.strip()
        if not value:
            return None

        # Attempt to parse a datetime:
        if dateutil:
            try:
                return dateutil.parser.parse(value)
            except (TypeError, ValueError):
                return None

        # split usecs, because they are not recognized by strptime.
        if '.' in value:
            try:
                value, usecs = value.split('.')
                usecs = int(usecs)
            except ValueError:
                return None
        else:
            usecs = 0
        kwargs = {'microsecond': usecs}
        try:  # Seconds are optional, so try converting seconds first.
            return datetime.datetime(*time.strptime(value,
                                                    '%Y-%m-%d %H:%M:%S')[:6], **kwargs)
        except ValueError:
            try:  # Try without seconds.
                return datetime.datetime(*time.strptime(value,
                                                        '%Y-%m-%d %H:%M')[:5], **kwargs)
            except ValueError:  # Try without hour/minutes/seconds.
                try:
                    return datetime.datetime(*time.strptime(value,
                                                            '%Y-%m-%d')[:3], **kwargs)
                except ValueError:
                    return None

    def prepare_query_value(self, op, value):
        return super(DateTimeField, self).prepare_query_value(op, self.to_mongo(value))


class DateField(DateTimeField):
    def to_mongo(self, value):
        value = super(DateField, self).to_mongo(value)
        # drop hours, minutes, seconds
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(value.year, value.month, value.day)
        return value

    def to_python(self, value):
        value = super(DateField, self).to_python(value)
        # convert datetime to date
        if isinstance(value, datetime.datetime):
            value = datetime.date(value.year, value.month, value.day)
        return value


class EmbeddedDocumentField(BaseField):
    """An embedded document field - with a declared document_type.
    """

    def __init__(self, document_type, **kwargs):
        # XXX ValidationError raised outside of the "validate" method.
        if not (
            isinstance(document_type, six.string_types) or
            issubclass(document_type, EmbeddedDocument)
        ):
            self.error('Invalid embedded document class provided to an '
                       'EmbeddedDocumentField')

        self.document_type_obj = document_type
        super(EmbeddedDocumentField, self).__init__(**kwargs)

    @property
    def document_type(self):
        if isinstance(self.document_type_obj, six.string_types):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                resolved_document_type = self.owner_document
            else:
                resolved_document_type = get_document(self.document_type_obj)

            if not issubclass(resolved_document_type, EmbeddedDocument):
                # Due to the late resolution of the document_type
                # There is a chance that it won't be an EmbeddedDocument (#1661)
                self.error('Invalid embedded document class provided to an '
                           'EmbeddedDocumentField')
            self.document_type_obj = resolved_document_type

        return self.document_type_obj

    def to_python(self, value):
        if not isinstance(value, self.document_type):
            return self.document_type._from_son(value)
        return value

    def to_mongo(self, value, fields=None):
        if not isinstance(value, self.document_type):
            return value
        return self.document_type.to_mongo(value, fields)

    def validate(self, value, clean=True):
        """Make sure that the document instance is an instance of the
        EmbeddedDocument subclass provided when the document was defined.
        """
        # Using isinstance also works for subclasses of self.document
        if not isinstance(value, self.document_type):
            self.error('Invalid embedded document instance provided to an '
                       'EmbeddedDocumentField')
        self.document_type.validate(value, clean)

    def lookup_member(self, member_name):
        return self.document_type._fields.get(member_name)

    def prepare_query_value(self, op, value):
        if value is not None and not isinstance(value, self.document_type):
            try:
                value = self.document_type._from_son(value)
            except ValueError:
                raise InvalidQueryError("Querying the embedded document '%s' failed, due to an invalid query value" %
                                        (self.document_type._class_name,))
        super(EmbeddedDocumentField, self).prepare_query_value(op, value)
        return self.to_mongo(value)


class ListField(ComplexBaseField):
    """A list field that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.

    .. note::
        Required means it cannot be empty - as the default for ListFields is []
    """

    def __init__(self, field=None, **kwargs):
        self.field = field
        kwargs.setdefault('default', lambda: [])
        super(ListField, self).__init__(**kwargs)

    def __get__(self, instance, owner):
        if instance is None:
            # Document class being used rather than a document object
            return self
        value = instance._data.get(self.name)
        return super(ListField, self).__get__(instance, owner)

    def validate(self, value):
        """Make sure that a list of valid fields is being used."""
        if not isinstance(value, (list, tuple)):
            self.error('Only lists and tuples may be used in a list field')
        super(ListField, self).validate(value)

    def prepare_query_value(self, op, value):
        if self.field:

            # If the value is iterable and it's not a string nor a
            # BaseDocument, call prepare_query_value for each of its items.
            if (
                op in ('set', 'unset', None) and
                hasattr(value, '__iter__') and
                not isinstance(value, six.string_types) and
                not isinstance(value, BaseDocument)
            ):
                return [self.field.prepare_query_value(op, v) for v in value]

            return self.field.prepare_query_value(op, value)

        return super(ListField, self).prepare_query_value(op, value)


class EmbeddedDocumentListField(ListField):
    """Designed specially to hold a list of
    embedded documents to provide additional query helpers.
    """

    def __init__(self, document_type, **kwargs):
        """
        :param document_type: The type of
         :class:`EmbeddedDocument` the list will hold.
        :param kwargs: Keyword arguments passed directly into the parent
         :class:`ListField`.
        """
        super(EmbeddedDocumentListField, self).__init__(
            field=EmbeddedDocumentField(document_type), **kwargs
        )


class SortedListField(ListField):
    """A ListField that sorts the contents of its list before writing to
    the database in order to ensure that a sorted list is always
    retrieved.

    .. warning::
        There is a potential race condition when handling lists.  If you set /
        save the whole list then other processes trying to save the whole list
        as well could overwrite changes.  The safest way to append to a list is
        to perform a push operation.
    """

    _ordering = None
    _order_reverse = False

    def __init__(self, field, **kwargs):
        if 'ordering' in kwargs.keys():
            self._ordering = kwargs.pop('ordering')
        if 'reverse' in kwargs.keys():
            self._order_reverse = kwargs.pop('reverse')
        super(SortedListField, self).__init__(field, **kwargs)

    def to_mongo(self, value, fields=None):
        value = super(SortedListField, self).to_mongo(
            value, fields)
        if self._ordering is not None:
            return sorted(value, key=itemgetter(self._ordering),
                          reverse=self._order_reverse)
        return sorted(value, reverse=self._order_reverse)


def key_not_string(d):
    """Helper function to recursively determine if any key in a
    dictionary is not a string.
    """
    for k, v in d.items():
        if not isinstance(k, six.string_types) or (isinstance(v, dict) and key_not_string(v)):
            return True


def key_has_dot_or_dollar(d):
    """Helper function to recursively determine if any key in a
    dictionary contains a dot or a dollar sign.
    """
    for k, v in d.items():
        if ('.' in k or k.startswith('$')) or (isinstance(v, dict) and key_has_dot_or_dollar(v)):
            return True


class DictField(ComplexBaseField):
    """A dictionary field that wraps a standard Python dictionary. This is
    similar to an embedded document, but the structure is not defined.

    .. note::
        Required means it cannot be empty - as the default for DictFields is {}
    """

    def __init__(self, field=None, *args, **kwargs):
        self.field = field

        kwargs.setdefault('default', lambda: {})
        super(DictField, self).__init__(*args, **kwargs)

    def validate(self, value):
        """Make sure that a list of valid fields is being used."""
        if not isinstance(value, dict):
            self.error('Only dictionaries may be used in a DictField')

        if key_not_string(value):
            msg = ('Invalid dictionary key - documents must '
                   'have only string keys')
            self.error(msg)
        if key_has_dot_or_dollar(value):
            self.error('Invalid dictionary key name - keys may not contain "."'
                       ' or startswith "$" characters')
        super(DictField, self).validate(value)

    def lookup_member(self, member_name):
        return DictField(db_field=member_name)

    def prepare_query_value(self, op, value):
        match_operators = ['contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith',
                           'exact', 'iexact']

        if op in match_operators and isinstance(value, six.string_types):
            return StringField().prepare_query_value(op, value)

        # Used for instance when using DictField(ListField(IntField()))
        if hasattr(self.field, 'field'):
            if op in ('set', 'unset') and isinstance(value, dict):
                return {
                    k: self.field.prepare_query_value(op, v)
                    for k, v in value.items()
                }
            return self.field.prepare_query_value(op, value)

        return super(DictField, self).prepare_query_value(op, value)


class MapField(DictField):
    """A field that maps a name to a specified field type. Similar to
    a DictField, except the 'value' of each item must match the specified
    field type.
    """

    def __init__(self, field=None, *args, **kwargs):
        # XXX ValidationError raised outside of the "validate" method.
        if not isinstance(field, BaseField):
            self.error('Argument to MapField constructor must be a valid '
                       'field')
        super(MapField, self).__init__(field=field, *args, **kwargs)


class BinaryField(BaseField):
    """A binary data field."""

    def __init__(self, max_bytes=None, **kwargs):
        self.max_bytes = max_bytes
        super(BinaryField, self).__init__(**kwargs)

    def __set__(self, instance, value):
        """Handle bytearrays in python 3.1"""
        if six.PY3 and isinstance(value, bytearray):
            value = six.binary_type(value)
        return super(BinaryField, self).__set__(instance, value)

    def to_mongo(self, value):
        return Binary(value)

    def validate(self, value):
        if not isinstance(value, (six.binary_type, Binary)):
            self.error('BinaryField only accepts instances of '
                       '(%s, %s, Binary)' % (
                           six.binary_type.__name__, Binary.__name__))

        if self.max_bytes is not None and len(value) > self.max_bytes:
            self.error('Binary value is too long')

    def prepare_query_value(self, op, value):
        if value is None:
            return value
        return super(BinaryField, self).prepare_query_value(
            op, self.to_mongo(value))


class UUIDField(BaseField):
    """A UUID field.
    """
    _binary = None

    def __init__(self, binary=True, **kwargs):
        """
        Store UUID data in the database

        :param binary: if False store as a string.
        """
        self._binary = binary
        super(UUIDField, self).__init__(**kwargs)

    def to_python(self, value):
        if not self._binary:
            original_value = value
            try:
                if not isinstance(value, six.string_types):
                    value = six.text_type(value)
                return uuid.UUID(value)
            except (ValueError, TypeError, AttributeError):
                return original_value
        return value

    def to_mongo(self, value):
        if not self._binary:
            return six.text_type(value)
        elif isinstance(value, six.string_types):
            return uuid.UUID(value)
        return value

    def prepare_query_value(self, op, value):
        if value is None:
            return None
        return self.to_mongo(value)

    def validate(self, value):
        if not isinstance(value, uuid.UUID):
            if not isinstance(value, six.string_types):
                value = str(value)
            try:
                uuid.UUID(value)
            except (ValueError, TypeError, AttributeError) as exc:
                self.error('Could not convert to UUID: %s' % exc)
