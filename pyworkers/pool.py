import time
import queue
import logging
import multiprocessing as mp
import multiprocessing.connection

from .worker import Worker
from .persistent import PersistentWorker, WorkerType
from .utils import BraceStyleAdapter, Pipe

logger = BraceStyleAdapter(logging.getLogger(__name__))


class Pool():
    def __init__(self, target, results_queue=None, args=None, kwargs=None, close_timeout=5, force_terminate=None, name=None):
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
        if name is None:
            name = '{} worker {}'.format(self._name, len(self._workers))
        if userid is None:
            userid = len(self._workers)

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

            if worker.id in self._workers:
                raise ValueError(f'Duplicated worker id: {worker.id}')

            self._workers[worker.id] = worker
            self._queues[worker.id] = queue.parent_end
            self.handle_new_worker(worker)
            worker = None
        except:
            if worker: # hasn't been added to the pool yet, clean it up
                worker.terminate()
            raise

    def attach(self, worker):
        if not isinstance(worker, Worker):
            raise ValueError('Worker expected')

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

        logger.info('Closing pool: {!r}{}', self._name, '' if graceful else ' due to error')
        timeout = timeout if timeout is not None else self.timeout
        force = force if force is not None else self.force

        force_args = {}
        if force is not None:
            force_args['force'] = force

        for worker in self._workers.values():
            try:
                if not worker.is_alive():
                    if worker.has_error:
                        logger.debug('Worker {} has already died with error: {}', worker.id, worker.error)
                    else:
                        logger.debug('Worker {} already closed', worker.id)

                    continue

                alive = True
                worker.close()
                if graceful or not self._pending_per_worker.get(worker.id, None):
                    alive = not worker.wait(timeout=timeout)
                else:
                    alive = not worker.wait(timeout=0.1)

                if alive and (force is not False or not graceful):
                    logger.info('Terminating worker {}', worker.id)
                    worker.terminate(timeout=timeout, **force_args)
                else:
                    logger.debug('Worker {} closed gracefully', worker.id)

            except Exception:
                logger.exception('Error occurred while {} {}', 'closing' if graceful else 'terminating', worker)

        for q in self._queues.values():
            q.close()

        self._queues.clear()
        self._pool_closed = True

    def close(self, timeout=None, force=None):
        return self._close(timeout, force, True)

    def terminate(self, timeout=None, force=None):
        return self._close(timeout, force, False)


    def run(self, *input_sources, results_callback=None, enqueue_callback=None, worker_extra_pending_inputs=0, return_results=True):
        if self._pool_closed:
            raise RuntimeError('Trying to use a closed Pool')
        if self._map_guard:
            raise RuntimeError('recursive map!')

        try:
            self._map_guard = True
            self._depleted = False
            self._pending = 0
            self._closed = set()
            self._finished = set()
            self._pending_per_worker = { worker.id: 0 for worker in self.workers }
            ret = []

            def next_inputs(worker):
                if self._depleted:
                    return None
                try:
                    return tuple([source(worker) if callable(source) else next(source) for source in input_sources])
                except StopIteration:
                    logger.debug('At least one input sequence has been depleted - no more data will be enqueued to any worker')
                    self._depleted = True
                    return None

            def enqueue(worker):
                trials = 0
                inp = next_inputs(worker)
                while True:
                    trials += 1
                    if not self._depleted:
                        if worker.id in self._closed:
                            logger.warning('Requested to enqueue new data to {} but the worker is closed - enqueue will be ignored', worker)
                            return False, False
                        if worker.id in self._finished:
                            logger.warning('Requested to enqueue new data to {} but the worker has already finished running - enqueue will be ignored', worker)
                            return False, False

                        logger.debug('Enqueuing new data to {}', worker)
                        try:
                            if enqueue_callback:
                                if not enqueue_callback(worker, *inp):
                                    logger.debug('Enqueue callback returned False, closing worker {}', worker)
                                    self._closed.add(worker.id)
                                    return False, True
                            else:
                                worker.enqueue(*inp)
                        except:
                            time.sleep(0.3)
                            if not worker.is_alive():
                                self._pending -= self._pending_per_worker[worker.id]
                                logger.warning('{} died while enqueueing', worker)
                                self._finished.add(worker.id)
                                return False, False
                            else:
                                logger.debug('Enqueueing failed for current input and worker {} but the worker is still alive - will try next input', worker)
                                continue

                        self._pending += 1
                        self._pending_per_worker[worker.id] += 1
                        logger.debug('Current pending results: {}, for {} only: {}', self._pending, worker, self._pending_per_worker[worker.id])
                        return True, True
                    else:
                        logger.debug('Input depleted - closing {}', worker)
                        #worker.close()
                        self._closed.add(worker.id)
                        return False, True

            for _ in range(worker_extra_pending_inputs + 1):
                for worker in self._workers.values():
                    enqueue(worker)
                    if self._depleted:
                        break
                if self._depleted:
                    break

            while self._pending and set(self._get_all_workers_ids()).difference(self._finished):
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
                        logger.debug('Closing and forgetting output queue {}', self._queues[wid])
                        self._queues[wid].close()
                        del self._queues[wid]
                        if wid not in self._finished:
                            msg = (None, False, None, wid)
                            logger.debug('Artificial closing message from worker {} created', wid)
                        else:
                            continue

                    if msg is None:
                        logger.warning('Received None message - finishing the loop with {} pending executions and {} workers running', self._pending, len(set(self._workers.keys()).difference(self._finished)))
                        break

                    unused_counter, flag, result, wid = msg
                    assert wid in self._workers
                    worker = self._workers.get(wid, None)
                    if not flag:
                        logger.warning('{} has died', worker)
                        self._pending -= self._pending_per_worker[wid]
                        # self._pending_per_worker[wid]
                        logger.debug('Marking {} as finished', worker)
                        self._finished.add(worker.id)
                    else:
                        assert wid not in self._closed and wid not in self._finished
                        self._pending -= 1
                        self._pending_per_worker[wid] -= 1
                        logger.debug('New result received from {}, total pending: {}, for this worker: {}', worker, self._pending, self._pending_per_worker[wid])
                        if results_callback is not None:
                            result = results_callback(worker, result)
                        if return_results:
                            ret.append(result)
                        if worker.id not in self._closed:
                            logger.debug('Trying to enqueue new data for {}', worker)
                            more, alive = enqueue(worker)
                            if not more and alive and not self._pending_per_worker[wid]:
                                logger.debug('No more work to be done for {} - marking as finished', worker)
                                self._finished.add(worker.id)

            logger.debug('All workers has finished and/or died, Pool.run is finishing...')
        finally:
            self._map_guard = False

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
