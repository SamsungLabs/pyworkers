import random
import queue

from .test_utils import make_test, GenericTest

from pyworkers.worker import WorkerTerminatedError
from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.utils import active_sleep


def test_fun(x):
    return x**2


def malicious_test_fun(x):
    while True:
        try:
            active_sleep(10)
            return x**2
        except Exception:
            pass


class PersistentTerminateServerTest(GenericTest):
    def __init__(self, target_cls, *args, **kwargs):
        if not target_cls.is_remote:
            raise TypeError('Only remote types are expected got {}'.format(target_cls.__name__))
        super().__init__(target_cls, *args, **kwargs)

    def test_terminate(self):
        xs = [random.random() for _ in range(10)]
        self.worker = self.create_worker(test_fun)
        self.assertTrue(self.worker.is_alive())
        for x in xs:
            self.worker.enqueue(x)

        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                try:
                    y = self.worker.next_result()
                    self.assertEqual(x**2, y)
                except queue.Empty:
                    break

    def test_double_terminate(self):
        xs = [random.random() for _ in range(10)]
        self.worker = self.create_worker(test_fun)
        self.assertTrue(self.worker.is_alive())
        for x in xs:
            self.worker.enqueue(x)
        
        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNotNone(self.worker.error)
        self.assertIs(type(self.worker.error), WorkerTerminatedError)
        self.assertIsNone(self.worker.result)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                try:
                    y = self.worker.next_result()
                    self.assertEqual(x**2, y)
                except queue.Empty:
                    break

    def test_force_terminate(self):
        # we don't want to force terminate thread
        if self.target_cls.is_thread:
            return

        xs = [random.random() for _ in range(10)]
        self.worker = self.create_worker(malicious_test_fun)
        self.assertTrue(self.worker.is_alive())
        for x in xs:
            self.worker.enqueue(x)

        self.assertTrue(self.server.terminate(timeout=5, force=True))
        self.assertTrue(self.worker.terminate(timeout=0, force=False))
        self.assertTrue(self.worker.wait(0))
        self.assertFalse(self.worker.is_alive())

        self.assertTrue(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertIsNone(self.worker.result)

        counter = 0
        for i, x in enumerate(xs):
            with self.subTest(i=i):
                try:
                    y = self.worker.next_result()
                    self.assertEqual(x**2, y)
                    counter += 1
                except queue.Empty:
                    break

        self.assertEqual(counter, 0)


for cls in [PersistentRemoteWorker]:
    testtype = make_test(PersistentTerminateServerTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype
