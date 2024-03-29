import unittest

import os
import math
import signal

from .test_utils import GenericTest, make_test

from pyworkers.pool import Pool, PoolError
from pyworkers.persistent import PersistentWorker
from pyworkers.worker import WorkerType, WorkerTerminatedError
from pyworkers.persistent_thread import PersistentThreadWorker
from pyworkers.persistent_process import PersistentProcessWorker
from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.utils import active_sleep


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


def fails_for_some_x(x):
    return 1/x


def fails_for_some_x_heavy(x):
    active_sleep(0.1)
    return 1/x


def fails_on_worker_0(wid, x):
    if not wid:
        raise SuicideError()

    return x**2


def fails_on_worker_0_heavy(wid, x):
    if not wid:
        raise SuicideError()

    active_sleep(0.1)
    return x**2


# This class implements standard persistent behaviour,
# however if a SuicideError happens when evaluating the target function
# it ignores it and continues its work
# it is used to test the case when a custom persistant class
# handles exceptions raised in do_work - if it does, the class
# should work fine
class ResilientPersistent(PersistentWorker):
    def __init__(self, target, *args, host=None, **kwargs):
        self._raise = True
        if type(self).is_remote:
            super().__init__(target, *args, host=host, **kwargs)
        else:
            super().__init__(target, *args, **kwargs)


    def do_work(self):
        while True:
            try:
                return super().do_work()
            except SuicideError as e:
                self._send_result(e)


class ResilientPersistent_Thread(ResilientPersistent, PersistentThreadWorker):
    pass

class ResilientPersistent_Process(ResilientPersistent, PersistentProcessWorker):
    pass

class ResilientPersistent_Remote(ResilientPersistent, PersistentRemoteWorker):
    pass





class PoolTest(GenericTest):
    def __init__(self, target_cls, *args, **kwargs):
        super().__init__(target_cls, *args, **kwargs)

    def test_simple(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

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
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))
            results2 = p.run(iter(i for i in range(10, 20)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        self.assertEqual(sum(w.result for w in p.workers), 20) # collectively, worker have processed 20 requests

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

    def test_restart(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter(i for i in range(10)))
            p.restart_workers()
            results2 = p.run(iter(i for i in range(10, 20)))

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        self.assertEqual(sum(w.result for w in p.workers), 10) # due to a restart, collectively, worker are only aware of the second 10 requests

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
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(10)))
                except PoolError as e:
                    results = e.partial_results
                    raise

        self.assertEqual(len(p.workers), 3)
        self.assertFalse(results)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIs(type(w.error), SuicideError)

    def test_midlife_crisis_suicide(self):
        p = Pool(midlife_crisis_suicide_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(10)))
                except PoolError as e:
                    results = e.partial_results
                    raise

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

    def test_midlife_crisis_suicide_resilient(self):
        worker_cls = None
        if self.target_cls == WorkerType.THREAD:
            worker_cls = ResilientPersistent_Thread
        elif self.target_cls == WorkerType.PROCESS:
            worker_cls = ResilientPersistent_Process
        elif self.target_cls == WorkerType.REMOTE:
            worker_cls = ResilientPersistent_Remote

        p = Pool(midlife_crisis_suicide_fn, name='Test Pool')
        with p:
            p.add_worker(worker_cls, host=self.server_addr, name=f'Worker_0', userid=0)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            results = p.run(iter([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]))

        self.assertEqual(len(p.workers), 1)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertFalse(w.has_error)

        check = []
        errors = 0
        for r in results:
            if isinstance(r, SuicideError):
                errors += 1
                continue
            x = int(math.sqrt(r))
            check.append(x)

        self.assertEqual(errors, 2)
        self.assertEqual(list(sorted(check)), [1, 1, 2, 2, 3, 3, 4, 4])

    def test_genocide(self):
        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            for w in p.workers:
                self.assertTrue(w.terminate())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(10)))
                except PoolError as e:
                    results = e.partial_results
                    raise

        self.assertEqual(len(p.workers), 3)
        self.assertFalse(results)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsNotNone(w.error)
            self.assertIs(type(w.error), WorkerTerminatedError)

    def test_surprise_genocide(self):
        if self.target_cls == WorkerType.THREAD:
            return

        p = Pool(test_fn, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            for w in p.workers:
                # remote workers are using 127.0.0.1 so we are able to kill them using PID
                # we use SIGTERM rather than terminate to simulate a situation when the child
                # does not have opportunity to react to termination request
                os.kill(w.pid, signal.SIGTERM)

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(10)))
                except PoolError as e:
                    results = e.partial_results
                    raise

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
                    p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

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
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

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

    def test_retries(self):
        p = Pool(fails_for_some_x, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(3)))
                except PoolError as e:
                    results = e.partial_results
                    raise

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsInstance(w.error, ZeroDivisionError)

        check = set()
        for r in results:
            x = int(1/r)
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), [1, 2])

    def test_retries_heavy(self):
        p = Pool(fails_for_some_x_heavy, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(3)))
                except PoolError as e:
                    results = e.partial_results
                    raise

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsInstance(w.error, ZeroDivisionError)

        check = set()
        for r in results:
            x = int(1/r)
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), [1, 2])


    def test_retries_x2(self):
        p = Pool(fails_for_some_x, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            with self.assertRaisesRegex(PoolError, 'Pool failed to process the whole input'):
                try:
                    results = p.run(iter(i for i in range(6)), worker_extra_pending_inputs=1)
                except PoolError as e:
                    results = e.partial_results
                    raise

        self.assertEqual(len(p.workers), 3)
        for w in p.workers:
            self.assertFalse(w.is_alive())
            self.assertTrue(w.has_error)
            self.assertIsInstance(w.error, ZeroDivisionError)

        check = set()
        for r in results:
            x = int(1/r)
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), [1, 2, 3, 4, 5])

    def test_retry_one_death(self):
        p = Pool(fails_on_worker_0, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            def enqueue_fn(worker, *args):
                worker.enqueue(worker.userid, *args)
                return True

            results = p.run(iter(i for i in range(6)), enqueue_fn=enqueue_fn)

        self.assertEqual(len(p.workers), 3)
        for idx, w in enumerate(p.workers):
            self.assertFalse(w.is_alive())
            if not idx:
                self.assertTrue(w.has_error)
                self.assertIsInstance(w.error, SuicideError)
            else:
                self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(6)))

    def test_no_retry_one_death(self):
        p = Pool(fails_on_worker_0, name='Test Pool', retry=False)
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            def enqueue_fn(worker, *args):
                worker.enqueue(worker.userid, *args)
                return True

            results = p.run(iter(i for i in range(3)), enqueue_fn=enqueue_fn)

        self.assertEqual(len(p.workers), 3)
        for idx, w in enumerate(p.workers):
            self.assertFalse(w.is_alive())
            if not idx:
                self.assertTrue(w.has_error)
                self.assertIsInstance(w.error, SuicideError)
            else:
                self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), [1,2])

    def test_javier(self):
        p = Pool(fails_on_worker_0_heavy, name='Test Pool')
        with p:
            for i in range(3):
                p.add_worker(self.target_cls, host=self.server_addr, name=f'Worker_{i}', userid=i)

            for w in p.workers:
                self.assertTrue(w.is_alive())

            def enqueue_fn(worker, *args):
                print(worker.userid)
                worker.enqueue(worker.userid, *args)
                return True

            results = p.run(iter(i for i in range(3)), enqueue_fn=enqueue_fn)
            self.assertEqual(len(p.workers), 3)
            for idx, w in enumerate(p.workers):
                if not idx:
                    w.wait(0.5) # we need some time to detect that the process has died, even though we already detected it in the pool - this is because different mechanisms are used in those places: pool uses pipes (closed pipe == dead worker), is_alive checks if the process exists in the OS (pid) which seems to have higher latency
                    self.assertFalse(w.is_alive())
                    self.assertTrue(w.has_error)
                    self.assertIsInstance(w.error, SuicideError)
                else:
                    self.assertTrue(w.is_alive())
                    self.assertFalse(w.has_error)

            results2 = p.run(iter(i for i in range(3,6)), enqueue_fn=enqueue_fn)
            results.extend(results2)

        self.assertEqual(len(p.workers), 3)
        for idx, w in enumerate(p.workers):
            self.assertFalse(w.is_alive())
            if not idx:
                self.assertTrue(w.has_error)
                self.assertIsInstance(w.error, SuicideError)
            else:
                self.assertFalse(w.has_error)

        check = set()
        for r in results:
            x = int(math.sqrt(r))
            self.assertNotIn(x, check)
            check.add(x)

        self.assertEqual(list(sorted(check)), list(range(6)))


for cls in [WorkerType.THREAD, WorkerType.PROCESS, WorkerType.REMOTE]:
    testtype = make_test(PoolTest, cls)
    globals()[testtype.__name__] = testtype
    del testtype


if __name__ == '__main__':
    unittest.main()
