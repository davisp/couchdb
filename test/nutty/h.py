
import functools


import couch


class db(object):
    def __init__(self, name="test_suite_db"):
        self.name = name
    def __call__(self, fun):
        @functools.wraps(fun)
        def _wrapper(*args, **kwargs):
            srv = couch.Server()
            db = srv[self.name]
            return fun(db, *args, **kwargs)
        return _wrapper
