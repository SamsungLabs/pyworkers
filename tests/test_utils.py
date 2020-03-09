import os
import signal
import unittest


def make_test(testcls, undertestcls):
    t = type(testcls.__name__ + '({})'.format(undertestcls.__name__), (testcls, unittest.TestCase), { '__init__': lambda obj, *a: testcls.__init__(obj, undertestcls, *a) })
    t.__module__ = testcls.__module__
    return t


class GenericTest():
    def __init__(self, target_cls, *args, **kwargs):
        self.target_cls = target_cls
        self.worker = None
        self.server = None
        super().__init__(*args, **kwargs)

    def setUp(self):
        if self.target_cls.is_remote:
            from pyworkers.remote_server import spawn_server
            self.server = spawn_server('127.0.0.1', 6006)
            assert self.server.is_alive(), self.server.error

    def tearDown(self):
        if self.worker:
            self.worker.terminate(force=True)
            if self.worker.is_alive():
                os.kill(self.worker.pid, signal.SIGTERM)
        if self.server:
            #import time
            self.server.terminate(force=True)
            if self.server.is_alive():
                os.kill(self.server.pid, signal.SIGTERM)
        return super().tearDown()

    def create_worker(self, fn, *args, run=None, **kwargs):
        if self.target_cls.is_remote:
            return self.target_cls(target=fn, args=args, kwargs=kwargs, run=run, host='127.0.0.1:6006')

        return self.target_cls(target=fn, args=args, kwargs=kwargs, run=run)


def setUpModule():
    import sys
    from pathlib import Path
    new_path = Path(__file__).parents[1].absolute()
    sys.path = [new_path] + sys.path
    try:
        import pyworkers
    finally:
        sys.path = sys.path[1:]
