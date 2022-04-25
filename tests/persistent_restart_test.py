import random

from .test_utils import make_test, GenericTest

from pyworkers.persistent_thread import PersistentThreadWorker
from pyworkers.persistent_process import PersistentProcessWorker
from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.utils import active_sleep


def test_fun(x):
    return x**2


def test_fun_long(x):
    active_sleep(0.2)
    return x**2


class PersistentRestartTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _check_workload(self, num):
        xs = [random.random() for _ in range(num)]
        self.assertTrue(self.worker.is_alive())
        for x in xs:
            self.worker.enqueue(x)

        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())
        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, num)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                y = self.worker.next_result()
                self.assertEqual(x**2, y)

    def test_restart(self):
        self.worker = self.create_worker(test_fun)

        self._check_workload(10)
        wid = self.worker.id
        self.worker.restart()
        wid2 = self.worker.id
        self._check_workload(5)
        if not self.worker.is_thread:
            self.assertNotEqual(wid, wid2)



for cls in [PersistentThreadWorker, PersistentProcessWorker, PersistentRemoteWorker]:
    testtype = make_test(PersistentRestartTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    import unittest
    unittest.main()
