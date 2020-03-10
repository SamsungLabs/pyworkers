import unittest
import os
import signal
import random

from .test_utils import make_test, GenericTest

from pyworkers.worker import WorkerTerminatedError
from pyworkers.remote import RemoteWorker
from pyworkers.utils import active_sleep


def test_loop():
    print('Test loop!')
    while True:
        active_sleep(1, interval=0.1)


def malicious_loop():
    while True:
        try:
            test_loop()
        except Exception:
            pass


class TerminateServerTest(GenericTest):
    def __init__(self, target_cls, *args, **kwargs):
        if not target_cls.is_remote:
            raise TypeError('Only remote types are expected got {}'.format(target_cls.__name__))
        super().__init__(target_cls, *args, **kwargs)

    def test_terminate(self):
        self.worker = self.create_worker(test_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

    def test_double_terminate(self):
        self.worker = self.create_worker(test_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

    def test_force_terminate(self):
        self.worker = self.create_worker(malicious_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertIsNone(self.worker.result)

    def test_surprise_terminate(self):
        self.worker = self.create_worker(malicious_loop)
        self.assertTrue(self.worker.is_alive())
        os.kill(self.server.pid, signal.SIGTERM)

        self.assertTrue(self.server.wait(1))
        self.assertTrue(self.worker.wait(1))

        self.assertFalse(self.worker.is_alive())
        self.assertTrue(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertIsNone(self.worker.result)


for cls in [RemoteWorker]:
    testtype = make_test(TerminateServerTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
