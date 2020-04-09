import unittest

import os
import math
import signal

from .test_utils import GenericTest, make_test

from pyworkers.pool import Pool
from pyworkers.worker import WorkerType, WorkerTerminatedError


class SuicideError(Exception):
    pass


def test_fn(x):
    return x**2


def soft_suicide_fn(x):
    raise SuicideError()


def midlife_crisis_suicide_fn(x):
    if x >= 5:
        raise SuicideError()
    else:
        return x**2


class PoolTest(GenericTest):
    def __init__(self, target_cls, *args, **kwargs):
        super().__init__(target_cls, *args, **kwargs)

    def test_simple(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(10)))

    def test_double_run(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))
            results2 = p.run(iter(i for i in range(10, 20)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(10)))

        for r in results2:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(20)))

    def test_soft_suicide(self):
        p = Pool(soft_suicide_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIs(type(w.error), SuicideError)

        self.assertEqual(len(results), 0)

    def test_midlife_crisis_suicide(self):
        p = Pool(midlife_crisis_suicide_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIs(type(w.error), SuicideError)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(5)))

    def test_genocide(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            for w in p.workers:
                self.assertTrue(w.terminate())

            unused_results = p.run(iter(i for i in range(10)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsNotNone(w.error)
            print(w.error)
            self.assertIs(type(w.error), WorkerTerminatedError)

    def test_surprise_genocide(self):
        if self.target_cls == WorkerType.THREAD:
            return

        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            for w in p.workers:
                # remote workers are using 127.0.0.1 so we are able to kill them using PID
                # we use SIGTERM rather than terminate to simulate a situation when the child
                # does not have opportunity to react to termination request
                os.kill(w.pid, signal.SIGTERM)

            results = p.run(iter(i for i in range(10)))

        self.assertEqual(len(p.workers), 3)
        self.assertFalse(results)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsNone(w.error)


    def test_parent_suicide(self):
        p = Pool(test_fn, name='Test Pool')
        try:
            with p:
                for i in range(3):
                    p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

                for w in p.workers:
                    self.assertTrue(w.is_alive())

                raise SuicideError()

        except SuicideError:
            pass

        self.assertEqual(len(p.workers), 3)
        for i, w in enumerate(p.workers):
            with self.subTest(worker=i):
                self.assertFalse(w.is_alive())
                self.assertFalse(w.has_error)
                self.assertIsNone(w.error)

    def test_early_deplete(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(1)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(1)))


for cls in [WorkerType.THREAD, WorkerType.PROCESS, WorkerType.REMOTE]:
    testtype = make_test(PoolTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
