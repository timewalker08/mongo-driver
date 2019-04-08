from IPython.terminal.embed import InteractiveShellEmbed
from iu_mongo import connect, clear_all
from iu_mongo.base import BaseDocument, TopLevelDocumentMetaclass
import pprint
import argparse
from math import ceil


def partition(lst, size):
    return list(
        map(lambda x: lst[x * size:x * size + size],
            list(range(0, ceil(len(lst) / size)))))


BANNER = """
\033[95mAvailable commands are:\033[0m

%(commands)s 
"""

HOST_INFO = """
Host:   %(host)s
DB:     %(db)s
"""

DOCUMENTS_INFO = """
\033[95mAvailable Documents are:\033[0m

%(documents)s
"""


def pp(doc):
    if isinstance(doc, BaseDocument):
        pprint.pprint(doc.to_mongo(), indent=2)
    else:
        pprint.pprint(doc, indent=2)


class DBShell(object):
    def __init__(self, host=None, db=None):
        self._document_classes = []
        self._host = host
        self._db = db

    def start(self):
        ipshell = InteractiveShellEmbed(
            banner1="\033[95mWelcome to the DBShell!\033[0m",
            banner2="\033[95mThis is an interactive shell to manipulate your "
            "documents through iu_mongo driver\033[0m"
        )
        connection = self._connect
        h = self._help
        show_collections = self._show_collections
        command_docs = [
            '\033[94mconnection() -> get the current mongodb connection information\033[0m',
            '\033[94mconnection(host, db) -> re-connect to a new mongodb host and db\033[0m',
            '\033[94mpp(doc) -> show the document information\033[0m',
            '\033[94mh() -> get help information\033[0m',
            '\033[94mshow_collections() -> get all defined document classes to manipulate\033[0m'
        ]
        self._help_info = BANNER % {
            'commands': '\n\n'.join('\t%s' % command_doc for command_doc in command_docs)
        }
        self._connect(self._host, self._db)
        ipshell(self._help_info+"\n\n"+self._show_collections(display=False))

    def _help(self):
        print(self._help_info)
        self._show_collections()

    def _connect(self, host=None, db=None):
        if host and db:
            clear_all()
            connect(host, db_names=[db])
            self._host = host
            self._db = db
        print(HOST_INFO % {'host': self._host, 'db': self._db})

    def _show_collections(self, display=True):
        print_str = DOCUMENTS_INFO % {'documents': '\n'.join(
            ''.join('%-30s' %
                    doc_class.__name__
                    for doc_class in chunk)
            for chunk in partition(self._document_classes, 4)
        )}
        if display:
            print(print_str)
        else:
            return print_str

    def load_document_classes(self, doc_classes):
        if not isinstance(doc_classes, list):
            doc_classes = [doc_classes]
        for doc_class in doc_classes:
            if doc_class.__class__ != TopLevelDocumentMetaclass:
                continue
            if doc_class._meta['abstract']:
                continue
            if doc_class not in self._document_classes:
                globals()[doc_class.__name__] = doc_class
                self._document_classes.append(doc_class)

    @staticmethod
    def parse_module(module):
        import importlib
        import importlib.util
        if module.endswith('.py'):
            spec = importlib.util.spec_from_file_location('', module)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
        else:
            m = importlib.import_module(module)
        return list(m.__dict__.values())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost', help="MongoDB host")
    parser.add_argument('--db', default='test', help="DB connect to")
    parser.add_argument('module', help="module path (e.g. tests.model.testdoc)"
                        "can be a python module file path too(e.g. tests/model/testdoc.py)")
    args = parser.parse_args()
    dbshell = DBShell(args.host, args.db)
    dbshell.load_document_classes(DBShell.parse_module(args.module))
    dbshell.start()
