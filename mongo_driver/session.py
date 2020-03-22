from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from pymongo.errors import InvalidOperation
from mongo_driver.errors import TransactionError

__all__ = ['Session', 'TransactionContext']

DEFAULT_READ_CONCERN = ReadConcern('majority')
DEFAULT_WRITE_CONCERN = WriteConcern(w='majority', wtimeout=5000)
DEFAULT_READ_PREFERENCE = ReadPreference.PRIMARY


class TransactionContext(object):
    def __init__(self, pymongo_transaction_context, pymongo_session):
        self._pymongo_transaction_context = pymongo_transaction_context
        self._pymongo_session = pymongo_session

    def __enter__(self):
        self._pymongo_transaction_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pymongo_transaction_context.__exit__(exc_type, exc_val, exc_tb)

    @property
    def _transaction(self):
        return self._pymongo_session._transaction

    @property
    def transaction_id(self):
        return self._transaction.transaction_id


class Session(object):
    def __init__(self, pymongo_client_session):
        self._pymongo_client_session = pymongo_client_session

    @property
    def pymongo_session(self):
        return self._pymongo_client_session

    @property
    def pymongo_client(self):
        return self._pymongo_client_session.client

    @property
    def session_id(self):
        return self._pymongo_client_session.session_id

    def start_transaction(self):
        try:
            pymongo_transaction_context = self._pymongo_client_session.start_transaction(
                read_concern=DEFAULT_READ_CONCERN,
                write_concern=DEFAULT_WRITE_CONCERN,
                read_preference=DEFAULT_READ_PREFERENCE
            )
            return TransactionContext(pymongo_transaction_context, self._pymongo_client_session)
        except InvalidOperation as e:
            raise TransactionError(str(e))

    def abort_transaction(self):
        try:
            self._pymongo_client_session.abort_transaction()
        except InvalidOperation as e:
            raise TransactionError(str(e))

    def commit_transaction(self):
        self._pymongo_client_session.commit_transaction()

    def __enter__(self):
        self._pymongo_client_session.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._pymongo_client_session.__exit__(exc_type, exc_val, exc_tb)
