import unittest
import random

from .test_utils import make_test, GenericTest

from pyworkers.thread import ThreadWorker
from pyworkers.process import ProcessWorker
from pyworkers.remote import RemoteWorker
from pyworkers.persistent_thread import PersistentThreadWorker
from pyworkers.persistent_process import PersistentProcessWorker
from pyworkers.persistent_remote import PersistentRemoteWorker


def test_fun(x):
    return x**2


class StatefulWorker():
    def __init__(self, *args, init_state, **kwargs):
        super().__init__(*args, init_state=init_state, **kwargs)

    def run(self, *args, **kwargs):
        y = super().run(*args, **kwargs)
        y = y*self.user_state
        self.user_state += 1
        return y


class StatefulThreadWorker(StatefulWorker, ThreadWorker):
    pass

class StatefulProcessdWorker(StatefulWorker, ProcessWorker):
    pass

class StatefulRemoteWorker(StatefulWorker, RemoteWorker):
    pass

class StatefulPersistentThreadWorker(StatefulWorker, PersistentThreadWorker):
    pass

class StatefulPersistentProcessdWorker(StatefulWorker, PersistentProcessWorker):
    pass

class StatefulPersistentRemoteWorker(StatefulWorker, PersistentRemoteWorker):
    pass


class StateTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_basic(self):
        x = random.random()
        self.worker = self.create_worker(test_fun, x, init_state=1)
        if not self.worker.is_thread:
            self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
        self.assertTrue(self.worker.user_state == 1 or not self.worker.is_alive()) # if the worker is still alive, we should see the initial state
        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())
        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, x**2)
        self.assertEqual(self.worker.user_state, 2) # the change to the state should become visible on our side after the worker has finished

    def test_run_twice(self):
        x = random.random()
        self.worker = None

        expected_state = 1
        for run in range(2):
            with self.subTest(run=run):
                self.worker = self.create_worker(test_fun, x, init_state=expected_state if self.worker is None else self.worker.user_state)
                self.assertTrue(self.worker.is_alive() or self.worker.result is not None)
                if not self.worker.is_thread:
                    self.assertTrue(self.worker.user_state == expected_state or not self.worker.is_alive()) # if the worker is still alive, we should see the initial state
                self.assertTrue(self.worker.wait(1))
                self.assertFalse(self.worker.is_alive())
                self.assertFalse(self.worker.has_error)
                self.assertIsNone(self.worker.error)
                self.assertEqual(self.worker.result, x**2*expected_state)
                self.assertEqual(self.worker.user_state, expected_state+1) # the change to the state should become visible on our side after the worker has finished

                expected_state = self.worker.user_state


class PersistentStateTest(GenericTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_basic(self):
        xs = [random.random() for _ in range(10)]
        self.worker = self.create_worker(test_fun, init_state=1)
        self.assertTrue(self.worker.is_alive())
        for x in xs:
            self.worker.enqueue(x)

        if not self.worker.is_thread:
            self.assertEqual(self.worker.user_state, 1)

        results = []
        for _ in range(10):
            results.append(self.worker.next_result())

        if not self.worker.is_thread:
            # we've finished processing stuff but as long the worker is alive we should not see changes to the worker state
            self.assertEqual(self.worker.user_state, 1)

        self.assertTrue(self.worker.wait(1))
        self.assertFalse(self.worker.is_alive())
        self.assertFalse(self.worker.has_error)
        self.assertIsNone(self.worker.error)
        self.assertEqual(self.worker.result, 10)

        # now we should see the last state
        self.assertEqual(self.worker.user_state, 11)

        for i, x in enumerate(xs):
            with self.subTest(i=i):
                y = results[i]
                self.assertEqual(x**2*(i+1), y)

    def test_run_twice(self):
        xs = [random.random() for _ in range(10)]
        self.worker = None

        expected_state = 1
        for run in range(2):
            with self.subTest(run=run):
                self.worker = self.create_worker(test_fun, init_state=expected_state if self.worker is None else self.worker.user_state)
                self.assertTrue(self.worker.is_alive())
                for x in xs:
                    self.worker.enqueue(x)

                if not self.worker.is_thread:
                    self.assertEqual(self.worker.user_state, expected_state)

                results = []
                for _ in range(10):
                    results.append(self.worker.next_result())

                if not self.worker.is_thread:
                    # we've finished processing stuff but as long the worker is alive we should not see changes to the worker state
                    self.assertEqual(self.worker.user_state, expected_state)

                self.assertTrue(self.worker.wait(1))
                self.assertFalse(self.worker.is_alive())
                self.assertFalse(self.worker.has_error)
                self.assertIsNone(self.worker.error)
                self.assertEqual(self.worker.result, 10)

                # now we should see the last state
                self.assertEqual(self.worker.user_state, expected_state+10)

                for i, x in enumerate(xs):
                    with self.subTest(i=i):
                        y = results[i]
                        self.assertEqual(x**2*(i+expected_state), y)

                expected_state = self.worker.user_state

    def test_restart(self):
        xs = [random.random() for _ in range(10)]
        self.worker = None

        expected_state = 1
        for run in range(2):
            with self.subTest(run=run):
                if self.worker is None:
                    self.worker = self.create_worker(test_fun, init_state=expected_state)
                else:
                    if not self.worker.is_thread:
                        self.assertEqual(self.worker.user_state, expected_state)
                    self.worker.restart()
                    # restart should sync user_state
                    self.assertEqual(self.worker.user_state, expected_state+10)
                    expected_state = self.worker.user_state

                self.assertTrue(self.worker.is_alive())
                for x in xs:
                    self.worker.enqueue(x)

                if not self.worker.is_thread:
                    self.assertEqual(self.worker.user_state, expected_state)

                results = []
                for _ in range(10):
                    results.append(self.worker.next_result())

                if not self.worker.is_thread:
                    # we've finished processing stuff but as long the worker is alive we should not see changes to the worker state
                    self.assertEqual(self.worker.user_state, expected_state)

                for i, x in enumerate(xs):
                    with self.subTest(i=i):
                        y = results[i]
                        self.assertEqual(x**2*(i+expected_state), y)


for cls in [StatefulThreadWorker, StatefulProcessdWorker, StatefulRemoteWorker]:
    testtype = make_test(StateTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


for cls in [StatefulPersistentThreadWorker, StatefulPersistentProcessdWorker, StatefulPersistentRemoteWorker]:
    testtype = make_test(PersistentStateTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype



if __name__ == '__main__':
    unittest.main()
