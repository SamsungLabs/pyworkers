# Copyright 2022 Samsung Electronics Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import threading
import multiprocessing as mp

from .worker import Worker, WorkerType
from .persistent import PersistentWorker
from .utils import foreign_raise, get_logger, Pipe

logger = get_logger(__name__)


class PoolError(RuntimeError):
    def __init__(self, msg, partial_results=None):
        super().__init__(msg)
        self.partial_results = partial_results


class Pool():
    def __init__(self, target, results_queue=None, args=None, kwargs=None, retry=True, close_timeout=5, force_terminate=None, name=None):
        if close_timeout is not None and close_timeout < 0:
            raise ValueError('Negative timeout')
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._timeout = close_timeout
        self._force_close = None
        self._workers = {}
        self._queues = {}
        self._pool_closed = False
        self._name = name or type(self).__name__

        self._map_guard = False
        self._pending = 0
        self._depleted = False
        self._closed = set()
        self._pending_per_worker = {}
        self._retry = retry
        self._retries = []

        self._workers_lock = threading.Lock()
        self._next_worker_id = 0

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        if value is not None and value < 0:
            raise ValueError('Negative timeout')
        self._timeout = value

    @property
    def force(self):
        return self._force_close

    @force.setter
    def force(self, value):
        self._force_close = value

    @property
    def workers(self):
        return self._get_all_workers()

    def add_worker(self, worker_type, name=None, userid=None, target=None, args=None, kwargs=None, **worker_kwargs):
        worker = None
        with self._workers_lock:
            if name is None:
                name = '{} worker {}'.format(self._name, self._next_worker_id)
            if userid is None:
                userid = self._next_worker_id

            self._next_worker_id += 1

        try:
            queue = Pipe()
            worker_kwargs = {
                **worker_kwargs,
                'target': target or self._target,
                'args': args or self._args,
                'kwargs': kwargs or self._kwargs,
                'name': name,
                'userid': userid,
                'results_pipe': queue
            }

            if isinstance(worker_type, WorkerType):
                worker = PersistentWorker.create(worker_type, **worker_kwargs)
            else:
                worker = worker_type(**worker_kwargs)

            with self._workers_lock:
                if worker.id in self._workers:
                    raise ValueError(f'Duplicated worker id: {worker.id}')
                self._workers[worker.id] = worker
                self._queues[worker.id] = queue.parent_end

            self.handle_new_worker(worker)
        except:
            if worker:
                with self._workers_lock:
                    self._workers.pop(worker.id, None)
                    self._queues.pop(worker.id, None)

                worker.terminate()
            raise

        return worker

    def attach(self, worker):
        if not isinstance(worker, Worker):
            raise ValueError('Worker expected')

        with self._workers_lock:
            if worker.id in self._workers:
                return

            queue = worker.results_endpoint
            self._workers[worker.id] = worker
            self._queues[worker.id] = queue

        self.handle_new_worker(worker)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.close()
        else:
            self.terminate()

    def _close(self, timeout, force, graceful):
        if self._pool_closed:
            return
        if self._map_guard:
            raise RuntimeError('Requested to close the Pool while still processing workload, finish a call to Pool.run before calling Pool.close or Pool.terminate')

        logger.info('Closing pool: {!r}{}', self._name, '' if graceful else ' due to error')
        timeout = timeout if timeout is not None else self.timeout
        force = force if force is not None else self.force

        force_args = {}
        if force is not None:
            force_args['force'] = force

        def cleanup_worker(worker):
            try:
                if not worker.is_alive():
                    if worker.has_error:
                        logger.debug('Worker {} has already died with error: {}', worker.id, worker.error)
                    else:
                        logger.debug('Worker {} already closed', worker.id)

                    return

                alive = True
                worker.close()

                alive = not worker.wait(timeout=timeout)

                if alive and (force is not False or not graceful):
                    logger.info('Terminating worker {}', worker.id)
                    worker.terminate(timeout=timeout, **force_args)
                else:
                    logger.debug('Worker {} closed gracefully', worker.id)

            except Exception:
                logger.exception('Error occurred while {} {}', 'closing' if graceful else 'terminating', worker)

        _cleanup_jobs = []
        for worker in self._workers.values():
            t = threading.Thread(target=cleanup_worker, args=(worker,))
            t.start()
            _cleanup_jobs.append(t)

        try:
            for t in _cleanup_jobs:
                t.join()
        except:
            for t in _cleanup_jobs:
                if t.is_alive():
                    foreign_raise(t.ident, SystemExit)

            raise

        for q in self._queues.values():
            q.close()

        self._queues.clear()
        self._pool_closed = True

    def close(self, timeout=None, force=None):
        return self._close(timeout, force, True)

    def terminate(self, timeout=None, force=None):
        return self._close(timeout, force, False)

    def restart_workers(self, timeout=1, **kwargs):
        if self._pool_closed:
            raise RuntimeError('Trying to restart workers on a closed Pool')
        if self._map_guard:
            raise RuntimeError('Requested to restart workers while still processing workload, finish a call to Pool.run before calling Pool.restart_workers')

        to_restart = list(self._workers.items())

        for oldid, w in to_restart:
            queue = Pipe()
            w.restart(timeout=timeout, results_pipe=queue, **kwargs)
            del self._workers[oldid]
            self._queues.pop(oldid, None)
            self._workers[w.id] = w
            self._queues[w.id] = queue.parent_end

    def run(self, *input_sources, worker_callback=None, enqueue_fn=None, worker_extra_pending_inputs=0, return_results=True):
        if self._pool_closed:
            raise RuntimeError('Trying to use a closed Pool')
        if self._map_guard:
            raise RuntimeError('recursive map!')
        if not set(self._get_all_workers_ids()).difference(self._closed): # no workers
            return

        try:
            self._map_guard = True
            self._depleted = False
            self._pending = 0
            self._pending_per_worker = { worker.id: [] for worker in self.workers }
            self._retries = []
            ret = []

            def next_inputs(worker):
                ''' Return (new_data, from_retires, data)
                    if ``new_data`` is False, then ``data`` is ``None``
                    ``from_retries`` is ``True`` only if ``new_data`` is also ``True``
                    and its value comes from retrying mechanism
                '''
                if self._retries:
                    logger.debug('Retrying input that previous failed')
                    return True, True, self._retries.pop(0)

                if self._depleted:
                    return False, False, None

                try:
                    return True, False, tuple([source(worker) if callable(source) else next(source) for source in input_sources])
                except StopIteration:
                    logger.debug('At least one input sequence has been depleted - no extra data will be enqueued to any worker (re-enqueues can happen)')
                    self._depleted = True
                    return False, False, None

            def get_next_idle_worker():
                maybe_idle = set(wid for wid, workload in self._pending_per_worker.items() if not workload)
                idle = maybe_idle.difference(self._closed)
                if not idle:
                    return None
                return self._workers[next(iter(idle))]

            def handle_death(worker, when=None):
                if when:
                    logger.warning('{} died {}', worker, when)
                else:
                    logger.warning('{} died', worker)

                if self._retry:
                    self._retries.extend(self._pending_per_worker[worker.id])

                self._pending -= len(self._pending_per_worker[worker.id])
                self._pending_per_worker[worker.id].clear()
                self._closed.add(worker.id)
                if worker_callback:
                    worker_callback(worker, 'died')

                while self._retries:
                    idle = get_next_idle_worker()
                    if idle is None:
                        break

                    logger.debug('Found an idle worker: {}, trying to enqueue workload from previous failures worker to it', idle)
                    try_enqueue(idle)

            def handle_no_enqueue(worker, reason):
                logger.debug('Not enqueueing to the worker {}, reason: {}', worker, reason)
                if not self._pending_per_worker[worker.id]:
                    logger.debug('No more work to be done for worker {}, leaving idling', worker)
                    if worker_callback:
                        worker_callback(worker, 'idle')

            def handle_unused_data(data, from_retries):
                if not self._retry:
                    return
                if from_retries:
                    self._retries.insert(0, data)
                else:
                    self._retries.append(data)

            def handle_enqueue(worker, data):
                self._pending += 1
                self._pending_per_worker[worker.id].append(data)
                logger.debug('Current pending results: {}, for {} only: {}', self._pending, worker, len(self._pending_per_worker[worker.id]))
                if worker_callback:
                    worker_callback(worker, 'enqueued')

            def try_enqueue(worker):
                trials = 0
                has_data, from_retries, inp = next_inputs(worker)
                while True:
                    trials += 1
                    if has_data:
                        if worker.id in self._closed:
                            logger.warning('Requested to enqueue new data to {} but the worker does not accept more data (has either died or is already closed) - enqueue will be ignored', worker)
                            handle_unused_data(inp, from_retries)
                            return True

                        logger.debug('Enqueuing new data to {}', worker)
                        try:
                            if enqueue_fn:
                                if not enqueue_fn(worker, *inp):
                                    handle_no_enqueue(worker, 'user-provided enqueue function returned False')
                                    handle_unused_data(inp, from_retries)
                                    return True
                            else:
                                worker.enqueue(*inp)
                        except:
                            time.sleep(0.1)
                            if not worker.is_alive():
                                handle_death(worker, 'while enqueueing')
                                handle_unused_data(inp, from_retries)
                                return True
                            else:
                                logger.exception('Enqueueing failed for current input and worker {} but the worker is still alive - will try next input', worker)
                                continue

                        handle_enqueue(worker, inp)
                        return True
                    else:
                        handle_no_enqueue(worker, 'no more data')
                        return False

            def handle_new_result(worker, result):
                self._pending -= 1
                self._pending_per_worker[worker.id].pop(0)
                logger.debug('New result received from {}, total pending: {}, for this worker: {}', worker, self._pending, len(self._pending_per_worker[wid]))
                if worker_callback:
                    worker_callback(worker, 'finished', result)
                if return_results:
                    ret.append(result)
                if worker.id not in self._closed: # this is very unlikely to be False, but hypothetically can happen with a custom results_callback etc.
                    logger.debug('Trying to enqueue new data for {}', worker)
                    try_enqueue(worker)

            def first_enqueue():
                for _ in range(worker_extra_pending_inputs + 1):
                    for worker in self._workers.values():
                        if worker.id not in self._closed:
                            more_data = try_enqueue(worker)
                            if not more_data:
                                return

            first_enqueue()

            while self._pending and set(self._get_all_workers_ids()).difference(self._closed):
                ready = mp.connection.wait(list(self._get_all_queues()))
                for conn in ready:
                    if self._aux_connection(conn):
                        continue
                    try:
                        msg = conn.recv()
                    except EOFError:
                        logger.debug('EOFError occurred while reading from a pipe: {} - will try to issue artificial closing message', conn)
                        found = False
                        for wid, queue in self._queues.items():
                            if queue is conn:
                                found = True
                                break
                        assert found
                        if not found:
                            raise RuntimeError(f'Worker for the queue {conn} could not be found')
                        logger.debug('Closing and forgetting output queue {} of worker {}', self._queues[wid], wid)
                        self._queues[wid].close()
                        del self._queues[wid]
                        if wid not in self._closed:
                            msg = (None, False, None, wid)
                            logger.debug('Artificial closing message from worker {} created', wid)
                        else:
                            continue

                    if msg is None:
                        logger.warning('Received None message - finishing the loop with {} pending executions and {} workers running', self._pending, len(set(self._workers.keys()).difference(self._closed)))
                        break

                    unused_counter, flag, result, wid = msg
                    assert wid in self._workers
                    worker = self._workers.get(wid, None)
                    if not flag:
                        handle_death(worker)
                    else:
                        handle_new_result(worker, result)

            ok = (self._depleted and not self._pending and not self._retries)
            if ok:
                logger.debug('The whole workload has been processed, Pool.run is finishing with ok=True')
            else:
                logger.debug('All workers have finished and/or died but at least one input source is still available and/or pending results have not been received, Pool.run is finishing with ok=False (depleted: {}, pending: {}, len(retries): {})', self._depleted, self._pending, len(self._retries))
        finally:
            self._map_guard = False

        if not ok:
            raise PoolError('Pool failed to process the whole input - all workers have died', partial_results=(ret if return_results else None))

        if return_results:
            return ret

    def handle_new_worker(self, worker):
        pass

    def _aux_connection(self, conn):
        return False

    def _get_all_workers(self):
        return self._workers.values()

    def _get_all_workers_ids(self):
        return self._workers.keys()

    def _get_all_queues(self):
        return self._queues.values()
