import unittest
import random

from .test_utils import make_test, GenericTest

from pyworkers.thread import ThreadWorker
from pyworkers.process import ProcessWorker
from pyworkers.remote import RemoteWorker
from pyworkers.persistent_thread import PersistentThreadWorker
from pyworkers.persistent_process import PersistentProcessWorker
from pyworkers.persistent_remote import PersistentRemoteWorker


class NoRunTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        pass

    def test_target_none(self):
        self.worker = self.create_worker(None)
        self.assertTrue(self.worker.wait(0))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertFalse(self.worker.is_alive())

        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.result)
        self.assertIsNone(self.worker.error)

        if self.target_cls.is_persistent:
            self.assertEqual(len(list(self.worker.results_iter())), 0)

    def test_explicit(self):
        _guard = True
        def test():
            nonlocal _guard
            _guard = False
            return 'foo'

        self.worker = self.create_worker(test, run=False)
        self.assertTrue(self.worker.wait(0))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        if self.target_cls.is_persistent:
            self.worker.close()
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(_guard)

        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.result)
        self.assertIsNone(self.worker.error)

        if self.target_cls.is_persistent:
            self.assertEqual(len(list(self.worker.results_iter())), 0)


for cls in [ThreadWorker, ProcessWorker, RemoteWorker, PersistentThreadWorker, PersistentProcessWorker, PersistentRemoteWorker]:
    testtype = make_test(NoRunTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
