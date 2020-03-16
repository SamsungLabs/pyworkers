import unittest
import os
import signal
import random

from .test_utils import make_test, GenericTest

from pyworkers.worker import WorkerTerminatedError
from pyworkers.thread import ThreadWorker
from pyworkers.process import ProcessWorker
from pyworkers.remote import RemoteWorker
from pyworkers.utils import active_sleep


def test_loop():
    while True:
        active_sleep(1, interval=0.001)


def malicious_loop():
    while True:
        try:
            test_loop()
        except Exception:
            pass


class TerminateTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_terminate(self):
        self.worker = self.create_worker(test_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertTrue(self.worker.terminate(force=False))
        self.assertTrue(self.worker.wait())
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

    def test_double_terminate(self):
        self.worker = self.create_worker(test_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertTrue(self.worker.terminate(force=False))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait())
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

    def test_early_terminate(self):
        self.worker = self.create_worker(test_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertFalse(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

    def test_force_terminate(self):
        # we don't want to force terminate thread
        if self.target_cls.is_thread:
            return

        self.worker = self.create_worker(malicious_loop)
        self.assertTrue(self.worker.is_alive())
        self.assertFalse(self.worker.terminate(force=False))
        self.assertTrue(self.worker.terminate(force=True))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertIsNone(self.worker.result)

    def test_surprise_terminate(self):
        # we don't want to force terminate thread
        if self.target_cls.is_thread:
            return

        self.worker = self.create_worker(malicious_loop)
        self.assertTrue(self.worker.is_alive())
        os.kill(self.worker.pid, signal.SIGTERM)

        self.assertTrue(self.worker.wait(1))

        self.assertFalse(self.worker.is_alive())
        self.assertTrue(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertIsNone(self.worker.result)



for cls in [ThreadWorker, ProcessWorker, RemoteWorker]:
    testtype = make_test(TerminateTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
