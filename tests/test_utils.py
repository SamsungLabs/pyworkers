import os
import signal
import unittest

from pyworkers.worker import WorkerType


def make_test(test_cls, arg):
    if isinstance(arg, type):
        name = arg.__name__
    else:
        name = str(arg).rsplit('.', maxsplit=1)[-1]
    t = type(test_cls.__name__ + '({})'.format(name), (test_cls, unittest.TestCase), { '__init__': lambda obj, *a: test_cls.__init__(obj, arg, *a) })
    t.__module__ = test_cls.__module__
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
            self.server = spawn_server(('127.0.0.1', 60006))
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

        return super().tearDown() # pylint: disable=no-member

    def create_worker(self, fn, *args, run=None, **kwargs):
        return self.target_cls(target=fn, args=args, kwargs=kwargs, run=run)

    def assertTrue(self, *args, **kwargs):
        return super().assertTrue(*args, **kwargs) # pylint: disable=no-member
    def assertFalse(self, *args, **kwargs):
        return super().assertFalse(*args, **kwargs) # pylint: disable=no-member
    def assertEqual(self, *args, **kwargs):
        return super().assertEqual(*args, **kwargs) # pylint: disable=no-member
    def assertIs(self, *args, **kwargs):
        return super().assertIs(*args, **kwargs) # pylint: disable=no-member
    def assertIsNot(self, *args, **kwargs):
        return super().assertIsNot(*args, **kwargs) # pylint: disable=no-member
    def assertIsNone(self, *args, **kwargs):
        return super().assertIsNone(*args, **kwargs) # pylint: disable=no-member
    def assertIsNotNone(self, *args, **kwargs):
        return super().assertIsNotNone(*args, **kwargs) # pylint: disable=no-member
    def assertIsInstance(self, *args, **kwargs):
        return super().assertIsInstance(*args, **kwargs) # pylint: disable=no-member
    def assertIsNotInstance(self, *args, **kwargs):
        return super().assertIsNotInstance(*args, **kwargs) # pylint: disable=no-member
    def assertIn(self, *args, **kwargs):
        return super().assertIn(*args, **kwargs) # pylint: disable=no-member
    def assertNotIn(self, *args, **kwargs):
        return super().assertNotIn(*args, **kwargs) # pylint: disable=no-member
    def subTest(self, *args, **kwargs):
        return super().subTest(*args, **kwargs) # pylint: disable=no-member
