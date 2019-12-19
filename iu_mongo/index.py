import pymongo
from collections import OrderedDict

__all__ = ['KeyDirection', 'IndexProperty', 'IndexDefinition', 'TaggedIndex']


class KeyDirection(object):
    """
    Index key direction enum class
    """
    ASCENDING = 1
    DESCENDING = -1
    HASHED = 'hashed'
    TO_PYMONGO = {
        ASCENDING: pymongo.ASCENDING,
        DESCENDING: pymongo.DESCENDING,
        HASHED: pymongo.HASHED
    }
    NUM_TO_STR = {
        ASCENDING: '1',
        DESCENDING: '-1',
        HASHED: 'hashed',
    }
    STR_TO_NUM = {
        '1': ASCENDING,
        '-1': DESCENDING,
        'hashed': HASHED
    }


class IndexProperty(object):
    """
    Index property binary-bit class
    e.g. 2^enum (enum=0,1,2...)
    """
    UNIQUE = 1 << 0
    SPARSE = 1 << 1
    TTL = 1 << 2


class IndexDefinition(object):
    @staticmethod
    def _check_keys_and_get_property(keys, unique=False, sparse=False,
                                     expire_after_seconds=None):
        if not isinstance(keys, OrderedDict):
            raise Exception('Index keys must be defined in ordered dict')
        index_property = 0
        if unique:
            index_property += IndexProperty.UNIQUE
            for _, direction in keys.items():
                if direction == KeyDirection.HASHED:
                    raise Exception('Unique index must not be hashed index')
        if sparse:
            index_property += IndexProperty.SPARSE
        if expire_after_seconds is not None:
            index_property += IndexProperty.TTL
            if len(keys) > 1:
                raise Exception('TTL index must be a single-field index')
        return index_property

    @classmethod
    def parse_from_keys_str(cls, keys_str, unique=False, sparse=False,
                            expire_after_seconds=None, partial_filter_expression=None,  **kwargs):
        keys = []
        for key in keys_str.split(','):
            key_name, key_dir = key.split(':')
            if key_name == 'id':
                key_name = '_id'
            keys.append((key_name, KeyDirection.STR_TO_NUM[key_dir]))
        keys = OrderedDict(keys)
        return cls(keys, unique, sparse, expire_after_seconds, partial_filter_expression)

    def __init__(self, keys, unique=False, sparse=False, expire_after_seconds=None,
                 partial_filter_expression=None):
        self.keys = keys
        self.expire_after_seconds = expire_after_seconds
        self.partial_filter_expression = partial_filter_expression
        if len(self.keys) == 0:
            raise Exception('Empty keys definition')
        self.index_property =\
            self._check_keys_and_get_property(self.keys, unique=unique, sparse=sparse,
                                              expire_after_seconds=expire_after_seconds)

    def is_covered_by(self, other):
        from copy import copy
        length = len(self.keys)
        if length >= len(other.keys):
            return False
        tmp_keys1 = copy(self.keys)
        tmp_keys2 = copy(other.keys)
        for _ in range(length):
            if tmp_keys1.popitem(last=False) != tmp_keys2.popitem(last=False):
                return False
        return True

    def to_pymongo_keys(self):
        return [
            (key_name, KeyDirection.TO_PYMONGO[key_dir])
            for key_name, key_dir in self.keys.items()
        ]

    @property
    def name(self):
        return "_".join(["%s_%s" % (key_name, KeyDirection.NUM_TO_STR[key_dir])
                         for key_name, key_dir in self.keys.items()])

    @property
    def properties_str(self):
        ps = []
        if self.unique:
            ps.append('UNIQUE')
        if self.sparse:
            ps.append('SPARSE')
        if self.ttl:
            ps.append('TTL %d' % self.expire_after_seconds)
        return '(%s)' % (','.join(ps))

    @property
    def unique(self):
        return bool(IndexProperty.UNIQUE & self.index_property)

    @property
    def sparse(self):
        return bool(IndexProperty.SPARSE & self.index_property)

    @property
    def ttl(self):
        return bool(IndexProperty.TTL & self.index_property)

    def __str__(self):
        ps = []
        if self.unique:
            ps.append('UNIQUE')
        if self.sparse:
            ps.append('SPARSE')
        if self.ttl:
            ps.append('TTL')
        return '%s%s' % (self.name, '_'+'_'.join(ps) if ps else '')

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, IndexDefinition):
            return False
        return self.keys == other.keys and self.index_property == other.index_property


class TaggedIndex(IndexDefinition):
    class TagProperty(object):
        """
        tag propery bit class
        """
        DEFINED = 1 << 0
        BUILT = 1 << 1
        COVERED = 1 << 2

    @classmethod
    def parse_from_pymongo_index_def(cls, index_name, index_def):
        unique = sparse = expire_after_seconds = partial_filter_expression = None
        keys = []
        for k, v in index_def.items():
            if k == 'key':
                for key_name, key_dir in v:
                    if isinstance(key_dir, float):
                        key_dir = int(key_dir)
                    keys.append(
                        (key_name, KeyDirection.STR_TO_NUM[str(key_dir)]))
                keys = OrderedDict(keys)
            if k == 'unique':
                unique = bool(v)
            if k == 'sparse':
                sparse = bool(v)
            if k == 'expireAfterSeconds':
                expire_after_seconds = int(v)
            if k == 'partialFilterExpression':
                partial_filter_expression = dict(v)
        return cls(keys, real_name=index_name, unique=unique, sparse=sparse,
                   expire_after_seconds=expire_after_seconds,
                   partial_filter_expression=partial_filter_expression)

    @classmethod
    def parse_from_index_def(cls, index_def):
        return cls(keys=index_def.keys, unique=index_def.unique,
                   sparse=index_def.sparse,
                   expire_after_seconds=index_def.expire_after_seconds)

    @property
    def built(self):
        return bool(self.TagProperty.BUILT & self.tag_property)

    @property
    def defined(self):
        return bool(self.TagProperty.DEFINED & self.tag_property)

    @property
    def covered(self):
        return bool(self.TagProperty.COVERED & self.tag_property)

    def __init__(self, keys, real_name=None, **kwargs):
        super(TaggedIndex, self).__init__(keys, **kwargs)
        self.real_name = real_name
        self.tag_property = 0
        if self.real_name is not None:
            self.tag_property ^= self.TagProperty.BUILT
        else:
            self.tag_property ^= self.TagProperty.DEFINED
