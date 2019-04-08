import re
import warnings
import pymongo
import six
from six import iteritems

from iu_mongo.base import (BaseDocument,
                           DocumentMetaclass, EmbeddedDocumentList,
                           TopLevelDocumentMetaclass)
from iu_mongo.mixin.read_mixin import ReadMixin
from iu_mongo.mixin.write_mixin import WriteMixin

__all__ = ('Document', 'EmbeddedDocument')


class EmbeddedDocument(six.with_metaclass(DocumentMetaclass, BaseDocument)):
    """A :class:`~Document` that isn't stored in its own
    collection.  :class:`~EmbeddedDocument` should be used as
    fields on :class:`~Document` through the
    :class:`~EmbeddedDocumentField` field type.

    A :class:`~EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the driver interface).
    To enable this behaviour set :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.
    """

    __slots__ = ('_instance', )

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = DocumentMetaclass

    # A generic embedded document doesn't have any immutable properties
    # that describe it uniquely, hence it shouldn't be hashable. You can
    # define your own __hash__ method on a subclass if you need your
    # embedded documents to be hashable.
    __hash__ = None

    def __init__(self, *args, **kwargs):
        super(EmbeddedDocument, self).__init__(*args, **kwargs)
        self._instance = None
        self._changed_fields = []

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._data == other._data
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_mongo(self, *args, **kwargs):
        data = super(EmbeddedDocument, self).to_mongo(*args, **kwargs)

        # remove _id from the SON if it's in it and it's None
        if '_id' in data and data['_id'] is None:
            del data['_id']

        return data


class Document(six.with_metaclass(TopLevelDocumentMetaclass, BaseDocument, ReadMixin, WriteMixin)):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the driver interface).
    To enable this behaviourset :attr:`allow_inheritance` to ``True`` in the
    :attr:`meta` dictionary.

    By default, any extra attribute existing in stored data but not declared
    in your model will raise a :class:`~FieldDoesNotExist` error.
    This can be disabled by setting :attr:`strict` to ``False``
    in the :attr:`meta` dictionary.
    """

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass = TopLevelDocumentMetaclass

    __slots__ = ('__objects',)

    @property
    def pk(self):
        """Get the primary key."""
        if 'id_field' not in self._meta:
            return None
        return getattr(self, self._meta['id_field'])

    @pk.setter
    def pk(self, value):
        """Set the primary key."""
        return setattr(self, self._meta['id_field'], value)

    def __hash__(self):
        """Return the hash based on the PK of this document. If it's new
        and doesn't have a PK yet, return the default object hash instead.
        """
        if self.pk is None:
            return super(BaseDocument, self).__hash__()

        return hash(self.pk)

    def to_mongo(self, *args, **kwargs):
        data = super(Document, self).to_mongo(*args, **kwargs)

        # If '_id' is None, try and set it from self._data. If that
        # doesn't exist either, remote '_id' from the SON completely.
        if data['_id'] is None:
            if self._data.get('id') is None:
                del data['_id']
            else:
                data['_id'] = self._data['id']

        return data
