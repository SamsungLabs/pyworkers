import random

from .test_utils import make_test, GenericTest

from pyworkers.thread import ThreadWorker
from pyworkers.process import ProcessWorker
from pyworkers.remote import RemoteWorker
from pyworkers.utils import active_sleep


def test_fun(x):
    return x**2


def test_fun_long(x):
    active_sleep(2)
    return x**2


class GoodTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_simple_run(self):
        x = random.random()
        self.worker = self.create_worker(test_fun, x)
        self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())
        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, x**2)

    def test_double_wait(self):
        x = random.random()
        self.worker = self.create_worker(test_fun, x)
        self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())
        self.assertTrue(self.worker.wait())

    def test_simple_run_long(self):
        x = random.random()
        self.worker = self.create_worker(test_fun, x)
        self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
        self.assertTrue(self.worker.wait(4))
        self.assertFalse(self.worker.is_alive())
        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, x**2)

    def test_double_wait_long(self):
        x = random.random()
        self.worker = self.create_worker(test_fun, x)
        self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
        self.assertTrue(self.worker.wait(4))
        self.assertFalse(self.worker.is_alive())
        self.assertTrue(self.worker.wait())


for cls in [ThreadWorker, ProcessWorker, RemoteWorker]:
    testtype = make_test(GoodTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype
