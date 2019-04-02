from tests.model.testdoc import *
from iu_mongo import connect

if __name__ == '__main__':
    connect(db_names=['test'])
    import pdb
    pdb.set_trace()
