from mongo_driver.slave_ok_setting import *
from mongo_driver.document import *
from mongo_driver.fields import *
from mongo_driver.connection import *
from mongo_driver.errors import *
from mongo_driver.index import *
from mongo_driver.session import *
import mongo_driver.slave_ok_setting as slave_ok_setting
import mongo_driver.document as document
import mongo_driver.fields as fields
import mongo_driver.connection as connection
import mongo_driver.errors as errors
import mongo_driver.index as index
import mongo_driver.session as session
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
