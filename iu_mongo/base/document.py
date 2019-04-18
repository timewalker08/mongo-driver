import copy
import numbers
from functools import partial

from bson import DBRef, ObjectId, SON, json_util
import pymongo
import six
from six import iteritems

from iu_mongo.base.common import get_document
from iu_mongo.base.datastructures import (BaseDict, BaseList,
                                          EmbeddedDocumentList,
                                          StrictDict)
from iu_mongo.base.fields import ComplexBaseField
from iu_mongo.common import _import_class
from iu_mongo.errors import (FieldDoesNotExist, InvalidDocumentError,
                             LookUpError, OperationError, ValidationError)

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


class BaseDocument(object):
    __slots__ = ('_changed_fields', '_initialised', '_created', '_data',
                 '_auto_id_field', '_db_field_map',
                 '__weakref__')

    _dynamic = False
    STRICT = False

    def __init__(self, *args, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types
        :param values: A dictionary of values for the document
        """
        self._initialised = False
        self._created = True
        if args:
            # Combine positional arguments with named arguments.
            # We only want named arguments.
            field = iter(self._fields_ordered)
            # If its an automatic id field then skip to the first defined field
            if getattr(self, '_auto_id_field', False):
                next(field)
            for value in args:
                name = next(field)
                if name in values:
                    raise TypeError(
                        'Multiple values for keyword argument "%s"' % name)
                values[name] = value

        __auto_convert = values.pop('__auto_convert', True)

        # 399: set default values only to fields loaded from DB
        __only_fields = set(values.pop('__only_fields', values))

        _created = values.pop('_created', True)

        # Check if there are undefined fields supplied to the constructor,
        # if so raise an Exception.
        if self._meta.get('strict', True) or _created:
            _undefined_fields = set(values.keys()) - set(
                list(self._fields.keys()) + ['id', 'pk', '_cls', '_text_score'])
            if _undefined_fields:
                msg = (
                    'The fields "{0}" do not exist on the document "{1}"'
                ).format(_undefined_fields, self._class_name)
                raise FieldDoesNotExist(msg)

        if self.STRICT:
            self._data = StrictDict.create(allowed_keys=self._fields_ordered)()
        else:
            self._data = {}

        # Assign default values to instance
        for key, field in iteritems(self._fields):
            if self._db_field_map.get(key, key) in __only_fields:
                continue
            value = getattr(self, key, None)
            setattr(self, key, value)

        if '_cls' not in values:
            self._cls = self._class_name

        # Set passed values after initialisation
        for key, value in iteritems(values):
            key = self._reverse_db_field_map.get(key, key)
            if key in self._fields or key in ('id', 'pk', '_cls'):
                if __auto_convert and value is not None:
                    field = self._fields.get(key)
                    if field:
                        value = field.to_python(value)
                setattr(self, key, value)
            else:
                self._data[key] = value

        # Set any get_<field>_display methods
        self.__set_field_display()

        # Flag initialised
        self._initialised = True
        self._created = _created

    def __delattr__(self, *args, **kwargs):
        """Handle deletions of fields"""
        field_name = args[0]
        if field_name in self._fields:
            default = self._fields[field_name].default
            if callable(default):
                default = default()
            setattr(self, field_name, default)
        else:
            super(BaseDocument, self).__delattr__(*args, **kwargs)

    def __setattr__(self, name, value):
        try:
            self__created = self._created
        except AttributeError:
            self__created = True

        if (
            self._is_document and
            not self__created and
            name in self._meta.get('shard_key', tuple()) and
            self._data.get(name) != value
        ):
            msg = 'Shard Keys are immutable. Tried to update %s' % name
            raise OperationError(msg)

        try:
            self__initialised = self._initialised
        except AttributeError:
            self__initialised = False
        # Check if the user has created a new instance of a class
        if (self._is_document and self__initialised and
                self__created and name == self._meta.get('id_field')):
            super(BaseDocument, self).__setattr__('_created', False)

        super(BaseDocument, self).__setattr__(name, value)

    def __getstate__(self):
        data = {}
        for k in ('_changed_fields', '_initialised', '_created', '_fields_ordered'):
            if hasattr(self, k):
                data[k] = getattr(self, k)
        data['_data'] = self.to_mongo()
        return data

    def __setstate__(self, data):
        if isinstance(data['_data'], SON):
            data['_data'] = self.__class__._from_son(data['_data'])._data
        for k in ('_changed_fields', '_initialised', '_created', '_data'):
            if k in data:
                setattr(self, k, data[k])
        if '_fields_ordered' in data:
            _super_fields_ordered = type(self)._fields_ordered
            setattr(self, '_fields_ordered', _super_fields_ordered)

    def __iter__(self):
        return iter(self._fields_ordered)

    def __getitem__(self, name):
        """Dictionary-style field access, return a field's value if present.
        """
        try:
            if name in self._fields_ordered:
                return getattr(self, name)
        except AttributeError:
            pass
        raise KeyError(name)

    def __setitem__(self, name, value):
        """Dictionary-style field access, set a field's value.
        """
        # Ensure that the field exists before settings its value
        if name not in self._fields:
            raise KeyError(name)
        return setattr(self, name, value)

    def __contains__(self, name):
        try:
            val = getattr(self, name)
            return val is not None
        except AttributeError:
            return False

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        try:
            u = self.__str__()
        except (UnicodeEncodeError, UnicodeDecodeError):
            u = '[Bad Unicode data]'
        repr_type = str if u is None else type(u)
        return repr_type('<%s: %s>' % (self.__class__.__name__, u))

    def __str__(self):
        # TODO this could be simpler?
        if hasattr(self, '__unicode__'):
            if six.PY3:
                return self.__unicode__()
            else:
                return six.text_type(self).encode('utf-8')
        return six.text_type('%s object' % self.__class__.__name__)

    def __eq__(self, other):
        if isinstance(other, self.__class__) and hasattr(other, 'id') and other.id is not None:
            return self.id == other.id
        if isinstance(other, DBRef):
            return self._get_collection_name() == other.collection and self.id == other.id
        if self.id is None:
            return self is other
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def clean(self):
        """
        Hook for doing document level data cleaning before validation is run.

        Any ValidationError raised by this method will not be associated with
        a particular field; it will have a special-case association with the
        field defined by NON_FIELD_ERRORS.
        """
        pass

    def to_mongo(self, fields=None):
        """
        Return as SON data ready for use with MongoDB.
        """
        if not fields:
            fields = []

        data = SON()
        data['_id'] = None
        data['_cls'] = self._class_name

        # only root fields ['test1.a', 'test2'] => ['test1', 'test2']
        root_fields = {f.split('.')[0] for f in fields}

        for field_name in self:
            if root_fields and field_name not in root_fields:
                continue

            value = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if value is not None:
                f_inputs = field.to_mongo.__code__.co_varnames
                ex_vars = {}
                if fields and 'fields' in f_inputs:
                    key = '%s.' % field_name
                    embedded_fields = [
                        i.replace(key, '') for i in fields
                        if i.startswith(key)]

                    ex_vars['fields'] = embedded_fields

                value = field.to_mongo(value, **ex_vars)

            # Handle self generating fields
            if value is None and field._auto_gen:
                value = field.generate()
                self._data[field_name] = value

            if (value is not None) or (field.null):
                data[field.db_field] = value

        # Only add _cls if allow_inheritance is True
        if not self._meta.get('allow_inheritance'):
            data.pop('_cls')

        return data

    def validate(self, clean=True):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError as error:
                errors[NON_FIELD_ERRORS] = error

        # Get a list of tuples of field names and their current values
        fields = [(self._fields.get(name), self._data.get(name))
                  for name in self._fields_ordered]

        EmbeddedDocumentField = _import_class('EmbeddedDocumentField')

        for field, value in fields:
            if value is not None:
                try:
                    if isinstance(field, EmbeddedDocumentField):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError) as error:
                    errors[field.name] = error
            elif field.required and not getattr(field, '_auto_gen', False):
                errors[field.name] = ValidationError('Field is required',
                                                     field_name=field.name)

        if errors:
            pk = 'None'
            if hasattr(self, 'pk'):
                pk = self.pk
            elif self._instance and hasattr(self._instance, 'pk'):
                pk = self._instance.pk
            message = 'ValidationError (%s:%s) ' % (self._class_name, pk)
            raise ValidationError(message, errors=errors)

    def to_json(self, *args, **kwargs):
        """Convert this document to JSON.
        """
        return json_util.dumps(self.to_mongo(), *args, **kwargs)

    @classmethod
    def from_json(cls, json_data, created=False):
        """Converts json data to a Document instance

        :param json_data: The json data to load into the Document
        :param created: If True, the document will be considered as a brand new document
                        If False and an id is provided, it will consider that the data being
                        loaded corresponds to what's already in the database (This has an impact of subsequent call to .save())
                        If False and no id is provided, it will consider the data as a new document
                        (default ``False``)
        """
        return cls._from_son(json_util.loads(json_data), created=created)

    def _mark_as_changed(self, key):
        """Mark a key as explicitly changed by the user."""
        if not key:
            return

        if not hasattr(self, '_changed_fields'):
            return

        if '.' in key:
            key, rest = key.split('.', 1)
            key = self._db_field_map.get(key, key)
            key = '%s.%s' % (key, rest)
        else:
            key = self._db_field_map.get(key, key)

        if key not in self._changed_fields:
            levels, idx = key.split('.'), 1
            while idx <= len(levels):
                if '.'.join(levels[:idx]) in self._changed_fields:
                    break
                idx += 1
            else:
                self._changed_fields.append(key)
                # remove lower level changed fields
                level = '.'.join(levels[:idx]) + '.'
                remove = self._changed_fields.remove
                for field in self._changed_fields[:]:
                    if field.startswith(level):
                        remove(field)

    def _clear_changed_fields(self):
        """Using _get_changed_fields iterate and remove any fields that
        are marked as changed.
        """
        for changed in self._get_changed_fields():
            parts = changed.split('.')
            data = self
            for part in parts:
                if isinstance(data, list):
                    try:
                        data = data[int(part)]
                    except IndexError:
                        data = None
                elif isinstance(data, dict):
                    data = data.get(part, None)
                else:
                    data = getattr(data, part, None)

                if hasattr(data, '_changed_fields'):
                    if getattr(data, '_is_document', False):
                        continue
                    data._changed_fields = []

        self._changed_fields = []

    def _nestable_types_changed_fields(self, changed_fields, base_key, data):
        """Inspect nested data for changed fields

        :param changed_fields: Previously collected changed fields
        :param base_key: The base key that must be used to prepend changes to this data
        :param data: data to inspect for changes
        """
        # Loop list / dict fields as they contain documents
        # Determine the iterator to use
        if not hasattr(data, 'items'):
            iterator = enumerate(data)
        else:
            iterator = iteritems(data)

        for index_or_key, value in iterator:
            item_key = '%s%s.' % (base_key, index_or_key)
            # don't check anything lower if this key is already marked
            # as changed.
            if item_key[:-1] in changed_fields:
                continue

            if hasattr(value, '_get_changed_fields'):
                changed = value._get_changed_fields()
                changed_fields += ['%s%s' % (item_key, k)
                                   for k in changed if k]
            elif isinstance(value, (list, tuple, dict)):
                self._nestable_types_changed_fields(
                    changed_fields, item_key, value)

    def _get_changed_fields(self):
        """Return a list of all fields that have explicitly been changed.
        """
        EmbeddedDocument = _import_class('EmbeddedDocument')
        SortedListField = _import_class('SortedListField')

        changed_fields = []
        changed_fields += getattr(self, '_changed_fields', [])

        for field_name in self._fields_ordered:
            db_field_name = self._db_field_map.get(field_name, field_name)
            key = '%s.' % db_field_name
            data = self._data.get(field_name, None)
            field = self._fields.get(field_name)

            if db_field_name in changed_fields:
                # Whole field already marked as changed, no need to go further
                continue

            if isinstance(data, EmbeddedDocument):
                # Find all embedded fields that have been changed
                changed = data._get_changed_fields()
                changed_fields += ['%s%s' % (key, k) for k in changed if k]
            elif isinstance(data, (list, tuple, dict)):
                if isinstance(field, SortedListField) and field._ordering:
                    # if ordering is affected whole list is changed
                    if any(field._ordering in d._changed_fields for d in data):
                        changed_fields.append(db_field_name)
                        continue

                self._nestable_types_changed_fields(
                    changed_fields, key, data)
        return changed_fields

    @classmethod
    def _get_collection_name(cls):
        """Return the collection name for this class. None for abstract
        class.
        """
        return cls._meta.get('collection', None)

    @classmethod
    def _from_son(cls, son, only_fields=None, created=False):
        """Create an instance of a Document (subclass) from a PyMongo
        SON.
        """
        if not only_fields:
            only_fields = []

        if son and not isinstance(son, dict):
            raise ValueError(
                "The source SON object needs to be of type 'dict'")

        # Get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get('_cls', cls._class_name)

        # Convert SON to a data dict, making sure each key is a string and
        # corresponds to the right db field.
        data = {}
        for key, value in iteritems(son):
            key = str(key)
            key = cls._db_field_map.get(key, key)
            data[key] = value

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        changed_fields = []
        errors_dict = {}

        fields = cls._fields

        for field_name, field in iteritems(fields):
            if field.db_field in data:
                value = data[field.db_field]
                try:
                    data[field_name] = (value if value is None
                                        else field.to_python(value))
                    if field_name != field.db_field:
                        del data[field.db_field]
                except (AttributeError, ValueError) as e:
                    errors_dict[field_name] = e

        if errors_dict:
            errors = '\n'.join(['%s - %s' % (k, v)
                                for k, v in errors_dict.items()])
            msg = ('Invalid data to create a `%s` instance.\n%s'
                   % (cls._class_name, errors))
            raise InvalidDocumentError(msg)

        # In STRICT documents, remove any keys that aren't in cls._fields
        if cls.STRICT:
            data = {k: v for k, v in iteritems(data) if k in cls._fields}

        obj = cls(__auto_convert=False, _created=created,
                  __only_fields=only_fields, **data)
        obj._changed_fields = changed_fields

        return obj

    def __set_field_display(self):
        """For each field that specifies choices, create a
        get_<field>_display method.
        """
        fields_with_choices = [(n, f) for n, f in self._fields.items()
                               if f.choices]
        for attr_name, field in fields_with_choices:
            setattr(self,
                    'get_%s_display' % attr_name,
                    partial(self.__get_field_display, field=field))

    def __get_field_display(self, field):
        """Return the display value for a choice field"""
        value = getattr(self, field.name)
        if field.choices and isinstance(field.choices[0], (list, tuple)):
            if value is None:
                return None
            sep = getattr(field, 'display_sep', ' ')
            values = value if field.__class__.__name__ in (
                'ListField', 'SortedListField') else [value]
            return sep.join([
                six.text_type(dict(field.choices).get(val, val))
                for val in values or []])
        return value
