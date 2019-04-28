from iu_mongo.slave_ok_setting import *
from iu_mongo.document import *
from iu_mongo.fields import *
from iu_mongo.connection import *
from iu_mongo.errors import *
from iu_mongo.index import *
from iu_mongo.session import *
import iu_mongo.slave_ok_setting as slave_ok_setting
import iu_mongo.document as document
import iu_mongo.fields as fields
import iu_mongo.connection as connection
import iu_mongo.errors as errors
import iu_mongo.index as index
import iu_mongo.session as session
__author__ = 'Jiaye Zhu'

VERSION = (0, 1, 0)

__all__ = (list(document.__all__) + list(fields.__all__) +
           list(connection.__all__) + list(errors.__all__) +
           list(slave_ok_setting.__all__) + list(index.__all__) +
           list(session.__all__)
           )


def get_version():
    version = '%s.%s' % (VERSION[0], VERSION[1])
    if VERSION[2]:
        version = '%s.%s' % (version, VERSION[2])
    return version


__version__ = get_version()
