import tornado.ioloop
import tornado.web
import time
from tornado.gen import coroutine
from tests.model.testdoc import TestDoc
from iu_mongo.connection import connect


class MainHandler(tornado.web.RequestHandler):
    @coroutine
    def _get_docs(self):
        it = TestDoc.find_iter({})
        count = 0
        for _ in it:
            count += 1
            if count % 10000 == 0:
                yield
                print(count)

    @coroutine
    def get(self):
        start = time.time()
        yield self._get_docs()
        end = time.time()
        self.write({'time': (end - start) * 1000})


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])


if __name__ == "__main__":
    connect(db_names=['test'])
    TestDoc.remove({'test_pk': {'$gt': -1}})
    with TestDoc.bulk() as bulk_context:
        for i in range(50000):
            doc = TestDoc(test_pk=i, test_int=i,
                          test_str=str(i), test_list=[i])
            doc.bulk_save(bulk_context)
    app = make_app()
    app.listen(8888)
    print('Starting server...')
    tornado.ioloop.IOLoop.current().start()
