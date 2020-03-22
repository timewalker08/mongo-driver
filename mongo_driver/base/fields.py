import operator
import weakref

from bson import ObjectId
import pymongo
import six
from six import iteritems

from mongo_driver.base.common import UPDATE_OPERATORS
from mongo_driver.base.datastructures import (BaseDict, BaseList,
                                          EmbeddedDocumentList)
from mongo_driver.common import _import_class
from mongo_driver.errors import ValidationError


__all__ = ('BaseField', 'ComplexBaseField', 'ObjectIdField')


class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.
    """
    name = None
    _auto_gen = False  # Call `generate` to generate a value

    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for fields that implicitly
    # creates, creation_counter is used for all user-specified fields.
    creation_counter = 0
    auto_creation_counter = -1

    def __init__(self, required=False, default=None, validation=None, choices=None,
                 null=False, **kwargs):
        """
        :param required: If the field is required. Whether it has to have a
            value or not. Defaults to False.
        :param default: (optional) The default value for this field if no value
            has been set (or if the value has been unset).  It can be a
            callable.
        :param validation: (optional) A callable to validate the value of the
            field.  Generally this is deprecated in favour of the
            `FIELD.validate` method
        :param choices: (optional) The valid choices
        :param null: (optional) If the field value can be null. If no and there is a default value
            then the default value is set
        :param **kwargs: (optional) Arbitrary indirection-free metadata for
            this field can be supplied as additional keyword arguments and
            accessed as attributes of the field. Must not conflict with any
            existing attributes. Common metadata includes `verbose_name` and
            `help_text`.
        """
        self.db_field = ''
        self.required = required
        self.default = default
        self.validation = validation
        self.choices = choices
        self.null = null
        self._owner_document = None
        # Detect and report conflicts between metadata and base properties.
        conflicts = set(dir(self)) & set(kwargs)
        if conflicts:
            raise TypeError('%s already has attribute(s): %s' % (
                self.__class__.__name__, ', '.join(conflicts)))

        # Assign metadata to the instance
        # This efficient method is available because no __slots__ are defined.
        # self.__dict__.update(kwargs)

        # Adjust the appropriate creation counter, and save our local copy.
        if self.db_field == '_id':
            self.creation_counter = BaseField.auto_creation_counter
            BaseField.auto_creation_counter -= 1
        else:
            self.creation_counter = BaseField.creation_counter
            BaseField.creation_counter += 1

    def __get__(self, instance, owner):
        """Descriptor for retrieving a value from a field in a document.
        """
        if instance is None:
            # Document class being used rather than a document object
            return self

        # Get value from document instance if available
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        """Descriptor for assigning a value to a field in a document.
        """
        # If setting to None and there is a default
        # Then set the value to the default value
        if value is None:
            if self.null:
                value = None
            elif self.default is not None:
                value = self.default
                if callable(value):
                    value = value()

        if instance._initialised:
            try:
                if (self.name not in instance._data or
                        instance._data[self.name] != value):
                    instance._mark_as_changed(self.name)
            except Exception:
                # Values cant be compared eg: naive and tz datetimes
                # So mark it as changed
                instance._mark_as_changed(self.name)

        EmbeddedDocument = _import_class('EmbeddedDocument')
        if isinstance(value, EmbeddedDocument):
            value._instance = weakref.proxy(instance)
        elif isinstance(value, (list, tuple)):
            for v in value:
                if isinstance(v, EmbeddedDocument):
                    v._instance = weakref.proxy(instance)
        instance._data[self.name] = value

    def error(self, message='', errors=None, field_name=None):
        """Raise a ValidationError."""
        field_name = field_name if field_name else self.name
        raise ValidationError(message, errors=errors, field_name=field_name)

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type."""
        return value

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type."""
        return self.to_python(value)

    def _to_mongo_safe_call(self, value, fields=None):
        """Helper method to call to_mongo with proper inputs."""
        f_inputs = self.to_mongo.__code__.co_varnames
        ex_vars = {}
        if 'fields' in f_inputs:
            ex_vars['fields'] = fields

        return self.to_mongo(value, **ex_vars)

    def prepare_query_value(self, op, value):
        """Prepare a value that is being used in a query for PyMongo."""
        if op in UPDATE_OPERATORS:
            self.validate(value)
        return value

    def validate(self, value, clean=True):
        """Perform validation on a value."""
        pass

    def _validate_choices(self, value):
        Document = _import_class('Document')
        EmbeddedDocument = _import_class('EmbeddedDocument')

        choice_list = self.choices
        if isinstance(next(iter(choice_list)), (list, tuple)):
            # next(iter) is useful for sets
            choice_list = [k for k, _ in choice_list]

        # Choices which are other types of Documents
        if isinstance(value, (Document, EmbeddedDocument)):
            if not any(isinstance(value, c) for c in choice_list):
                self.error(
                    'Value must be an instance of %s' % (
                        six.text_type(choice_list)
                    )
                )
        # Choices which are types other than Documents
        else:
            values = value if isinstance(value, (list, tuple)) else [value]
            if len(set(values) - set(choice_list)):
                self.error('Value must be one of %s' %
                           six.text_type(choice_list))

    def _validate(self, value, **kwargs):
        # Check the Choices Constraint
        if self.choices:
            self._validate_choices(value)

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    self.error('Value does not match custom validation method')
            else:
                raise ValueError('validation argument for "%s" must be a '
                                 'callable.' % self.name)

        self.validate(value, **kwargs)

    @property
    def owner_document(self):
        return self._owner_document

    def _set_owner_document(self, owner_document):
        self._owner_document = owner_document

    @owner_document.setter
    def owner_document(self, owner_document):
        self._set_owner_document(owner_document)


class ComplexBaseField(BaseField):
    """Handles complex fields, such as lists / dictionaries.
    """

    field = None

    def __get__(self, instance, owner):
        if instance is None:
            # Document class being used rather than a document object
            return self
        EmbeddedDocumentListField = _import_class('EmbeddedDocumentListField')
        value = super(ComplexBaseField, self).__get__(instance, owner)
        # Convert lists / values so we can watch for any changes on them
        if isinstance(value, (list, tuple)):
            if (issubclass(type(self), EmbeddedDocumentListField) and
                    not isinstance(value, EmbeddedDocumentList)):
                value = EmbeddedDocumentList(value, instance, self.name)
            elif not isinstance(value, BaseList):
                value = BaseList(value, instance, self.name)
            instance._data[self.name] = value
        elif isinstance(value, dict) and not isinstance(value, BaseDict):
            value = BaseDict(value, instance, self.name)
            instance._data[self.name] = value
        return value

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type."""
        if isinstance(value, six.string_types):
            return value

        if hasattr(value, 'to_python'):
            return value.to_python()

        BaseDocument = _import_class('BaseDocument')
        if isinstance(value, BaseDocument):
            # Something is wrong, return the value as it is
            return value

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = {idx: v for idx, v in enumerate(value)}
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = {key: self.field.to_python(item)
                          for key, item in value.items()}
        else:
            value_dict = {}
            for k, v in value.items():
                if hasattr(v, 'to_python'):
                    value_dict[k] = v.to_python()
                else:
                    value_dict[k] = self.to_python(v)

        if is_list:  # Convert back to a list
            return [v for _, v in sorted(value_dict.items(),
                                         key=operator.itemgetter(0))]
        return value_dict

    def to_mongo(self, value, fields=None):
        """Convert a Python type to a MongoDB-compatible type."""
        Document = _import_class('Document')
        EmbeddedDocument = _import_class('EmbeddedDocument')

        if isinstance(value, six.string_types):
            return value

        if hasattr(value, 'to_mongo'):
            cls = value.__class__
            val = value.to_mongo(fields)
            # If it's a document that is not inherited add _cls
            if isinstance(value, EmbeddedDocument):
                val['_cls'] = cls.__name__
            return val

        is_list = False
        if not hasattr(value, 'items'):
            try:
                is_list = True
                value = {k: v for k, v in enumerate(value)}
            except TypeError:  # Not iterable return the value
                return value

        if self.field:
            value_dict = {
                key: self.field._to_mongo_safe_call(item, fields)
                for key, item in iteritems(value)
            }
        else:
            value_dict = {}
            for k, v in iteritems(value):
                if hasattr(v, 'to_mongo'):
                    cls = v.__class__
                    val = v.to_mongo(fields)
                    # If it's a document that is not inherited add _cls
                    if isinstance(v, (Document, EmbeddedDocument)):
                        val['_cls'] = cls.__name__
                    value_dict[k] = val
                else:
                    value_dict[k] = self.to_mongo(v, fields)

        if is_list:  # Convert back to a list
            return [v for _, v in sorted(value_dict.items(),
                                         key=operator.itemgetter(0))]
        return value_dict

    def validate(self, value):
        """If field is provided ensure the value is valid."""
        errors = {}
        if self.field:
            if hasattr(value, 'iteritems') or hasattr(value, 'items'):
                sequence = iteritems(value)
            else:
                sequence = enumerate(value)
            for k, v in sequence:
                try:
                    self.field._validate(v)
                except ValidationError as error:
                    errors[k] = error.errors or error
                except (ValueError, AssertionError) as error:
                    errors[k] = error

            if errors:
                field_class = self.field.__class__.__name__
                self.error('Invalid %s item (%s)' % (field_class, value),
                           errors=errors)
        # Don't allow empty values if required
        if self.required and not value:
            self.error('Field is required and cannot be empty')

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def lookup_member(self, member_name):
        if self.field:
            return self.field.lookup_member(member_name)
        return None

    def _set_owner_document(self, owner_document):
        if self.field:
            self.field.owner_document = owner_document
        self._owner_document = owner_document


class ObjectIdField(BaseField):
    """A field wrapper around MongoDB's ObjectIds."""

    def to_python(self, value):
        try:
            if not isinstance(value, ObjectId):
                value = ObjectId(value)
        except Exception:
            pass
        return value

    def to_mongo(self, value):
        if not isinstance(value, ObjectId):
            try:
                return ObjectId(six.text_type(value))
            except Exception as e:
                # e.message attribute has been deprecated since Python 2.6
                self.error(six.text_type(e))
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        try:
            ObjectId(six.text_type(value))
        except Exception:
            self.error('Invalid Object ID')
