from pymongo.mongo_client import MongoClient
from pymongo.read_preferences import ReadPreference
from iu_mongo.errors import ConnectionError
import collections

__all__ = ['connect', 'get_db', 'get_connection', 'clear_all', 'get_admin_db',
           'DEFAULT_WRITE_CONCERN', 'DEFAULT_WTIMEOUT']

_connections = {}
_dbs = {}
_db_to_conn = {}

DEFAULT_WRITE_CONCERN = 'majority'
DEFAULT_WTIMEOUT = 5000


def get_connection(conn_name="main"):
    return _connections.get(conn_name, None)


def get_db(db_name):
    global _dbs, _connections, _db_to_conn
    if db_name not in _dbs or not _dbs[db_name]:
        conn_name = _db_to_conn.get(db_name, None)
        conn = _connections.get(conn_name, None)
        _dbs[db_name] = conn and conn[db_name]
    return _dbs[db_name]


def get_admin_db(conn_name='main'):
    conn = _connections.get(conn_name, None)
    return conn.admin


def clear_all():
    global _connections, _dbs, _db_to_conn
    _connections = {}
    _dbs = {}
    _db_to_conn = {}


def connect(host='localhost', conn_name='main', db_names=[],
            port=27017, max_pool_size=None, w=DEFAULT_WRITE_CONCERN,
            wtimeout=DEFAULT_WTIMEOUT, socketTimeoutMS=None,
            connectTimeoutMS=None, waitQueueTimeoutMS=None,
            username=None, password=None, auth_db='admin', is_mock=False,
            replica_set=None):
    global _connections, _db_to_conn

    mongo_client_kwargs = {
        'host': host,
        'port': port,
        'w': w,
        'wtimeout': wtimeout,
        'maxPoolSize': max_pool_size,
        'socketTimeoutMS': socketTimeoutMS,
        'connectTimeoutMS': connectTimeoutMS,
        'waitQueueTimeoutMS': waitQueueTimeoutMS,
        # 'connect': False,
        'username': username,
        'password': password,
        'authSource': auth_db,
        'replicaSet': replica_set,
    }
    keys = [k for k in mongo_client_kwargs.keys()]
    for k in keys:
        if mongo_client_kwargs[k] is None:
            del mongo_client_kwargs[k]

    if is_mock:
        try:
            import mongomock
            client_class = mongomock.MongoClient
        except ImportError:
            raise RuntimeError('You need mongomock installed to mock mongodb')
    else:
        client_class = MongoClient
    # Connect to the database if not already connected
    if conn_name not in _connections:
        try:
            conn = client_class(**mongo_client_kwargs)
            # conn.admin.command('ismaster')
            _connections[conn_name] = conn
        except Exception as e:
            raise ConnectionError(
                'Cannot connect to the database: %s' % str(e))

        if db_names:
            for db in db_names:
                _db_to_conn[db] = conn_name

    return _connections[conn_name]
