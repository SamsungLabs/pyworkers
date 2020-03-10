import os
import signal
import unittest

from pyworkers.worker import WorkerType


def make_test(testcls, arg):
    if isinstance(arg, type):
        name = arg.__name__
    else:
        name = str(arg).rsplit('.', maxsplit=1)[-1]
    t = type(testcls.__name__ + '({})'.format(name), (testcls, unittest.TestCase), { '__init__': lambda obj, *a: testcls.__init__(obj, arg, *a) })
    t.__module__ = testcls.__module__
    return t


class GenericTest():
    def __init__(self, target_cls, *args, **kwargs):
        self.target_cls = target_cls
        self.worker = None
        self.server = None
        super().__init__(*args, **kwargs)

    def setUp(self):
        if self.target_cls == WorkerType.REMOTE or (not isinstance(self.target_cls, WorkerType) and self.target_cls.is_remote):
            from pyworkers.remote_server import spawn_server
            self.server = spawn_server('127.0.0.1', 6006)
            assert self.server.is_alive(), self.server.error

    def tearDown(self):
        if self.worker:
            self.worker.terminate(force=True)
            if self.worker.is_alive():
                os.kill(self.worker.pid, signal.SIGTERM)
        if self.server:
            self.server.terminate(force=True)
            if self.server.is_alive():
                os.kill(self.server.pid, signal.SIGTERM)
        return super().tearDown()

    def create_worker(self, fn, *args, run=None, **kwargs):
        return self.target_cls(target=fn, args=args, kwargs=kwargs, run=run)
