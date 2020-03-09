import multiprocessing as mp

from .persistent import PersistentWorker, WorkerType


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
        self._results = results_queue or mp.Queue()
        self._cleaned_up = True
        self._name = name or type(self).__name__

        self._map_guard = False
        self._pending = 0
        self._deplated = False
        self._closed = set()

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
        return self._workers.values()

    @property
    def results_queue(self):
        return self._results

    def add_worker(self, worker_type, name=None, userid=None):
        worker = None
        if name is None:
            name = '{} worker {}'.format(self._name, len(self._workers))
        if userid is None:
            userid = len(self._workers)

        try:
            worker_kwargs = {
                'target': self._target,
                'args': self._args,
                'kwargs': self._kwargs,
                'name': name,
                'userid': userid
            }

            worker = PersistentWorker.create(worker_type, **worker_kwargs)
            if worker.id in self._workers:
                raise ValueError(f'Duplicated worker id: {worker.id}')

            self._workers[worker.id] = worker
            worker = None
            self._cleaned_up = False
            self.handle_new_worker(worker)
        except:
            if worker: # hasn't been added to the pool yet, clean it up
                worker.terminate()
            raise

    def __enter__(self):
        pass

    def __exit__(self, *exc):
        if exc[0] is None:
            self.close()
        else:
            self.terminate()

    def close(self, timeout=None, force=None):
        if self._cleaned_up:
            return

        timeout = timeout if timeout is not None else self.timeout
        force = force if force is not None else self.force
        for worker in self.workers.values():
            worker.close()
            if not worker.wait(timeout=timeout) and force:
                worker.terminate(force=True)

        self._workers.clear()
        self._cleaned_up = True

    def terminate(self):
        if self._cleaned_up:
            return

        timeout = timeout if timeout is not None else self.timeout
        force = force if force is not None else self.force
        for worker in self.workers.values():
            worker.terminate(force=True)

        self._workers.clear()
        self._cleaned_up = True


    def map(self, results_callback, *seq_generators, worker_extra_pending_inputs=0):
        if self._map_guard:
            raise RuntimeError('recursive map!')

        self._map_guard = True
        self._deplated = False
        self._closed = set()

        def next_inputs():
            if self._deplated:
                return None
            try:
                return tuple(map(next, seq_generators))
            except StopIteration:
                self._deplated = True
                return None

        def enqueue(worker, close_if_deplated=False):
            inp = next_inputs()
            if not self._deplated:
                worker.enqueue(*inp)
            elif close_if_deplated:
                worker.close()
                self._closed.add(worker.id)

        
        for i in range(worker_extra_pending_inputs + 1):
            for worker in self._workers.values()
                if worker.id not in self._closed:
                    enqueue(worker, close_if_deplated=not i)

        while True:
            msg = self._results.get()
            if msg is None:
                break

            worker, result = msg
            results_callback(worker, result)
            if worker.id not in self._closed:
                enqueue(worker, close_if_deplated=True)


    def handle_new_worker(self, worker):
        pass

