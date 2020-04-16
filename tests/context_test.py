import unittest
import os
import queue
import signal
import random

from .test_utils import make_test, GenericTest

from pyworkers.worker import WorkerTerminatedError
from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.remote_context import RemoteContext
from pyworkers.utils import active_sleep, is_windows


def test_fun(x, exp=2):
    return x**exp

def long_test_fun(x, exp=2):
    if x >= 3:
        active_sleep(5)

    return x**exp


class PersistentContextTest(GenericTest):
    def __init__(self, target_cls, *args, **kwargs):
        if not target_cls.is_remote:
            raise TypeError('Only remote types are expected got {}'.format(target_cls.__name__))
        super().__init__(target_cls, *args, **kwargs)

    def test_good(self):
        self.context = RemoteContext(1, target=test_fun, kwargs={ 'exp': 3 })
        self.worker = PersistentRemoteWorker(None, host=self.context.host, context=self.context.context_id)
        self.assertTrue(self.worker.is_alive())

        xs = [random.random() for _ in range(10)]
        for x in xs:
            self.worker.enqueue(x)

        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.context.is_alive())
        self.assertTrue(self.context.wait())
        self.assertFalse(self.context.is_alive())

        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, 10)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                y = self.worker.next_result()
                self.assertEqual(x**3, y)

    def test_terminate_context(self):
        self.context = RemoteContext(1, target=long_test_fun, kwargs={ 'exp': 3 })
        self.worker = PersistentRemoteWorker(None, host=self.context.host, context=self.context.context_id)
        self.assertTrue(self.worker.is_alive())

        xs = [random.random() for _ in range(10)]
        for x in xs:
            self.worker.enqueue(x)

        self.assertTrue(self.context.wait())
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                try:
                    y = self.worker.next_result()
                    self.assertEqual(x**3, y)
                except queue.Empty:
                    break

    def test_terminate_empty_context(self):
        self.context = RemoteContext(1, target=long_test_fun, kwargs={ 'exp': 3 })
        self.assertTrue(self.context.is_alive())
        self.assertTrue(self.context.wait())
        self.assertFalse(self.context.is_alive())


for cls in [PersistentRemoteWorker]:
    testtype = make_test(PersistentContextTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
