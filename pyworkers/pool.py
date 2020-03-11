import multiprocessing as mp

from .persistent import PersistentWorker, WorkerType


class PipeToQueue():
    def __init__(self, pipe):
        self._pipe = pipe

    @property
    def pipe(self):
        return self._pipe

    def put(self, obj):
        return self._pipe.send(obj)

    def get(self):
        return self._pipe.recv()


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
            queue = mp.connection.Pipe()
            worker_kwargs = {
                'target': self._target,
                'args': self._args,
                'kwargs': self._kwargs,
                'name': name,
                'userid': userid,
                'results_queue': PipeToQueue(queue[0])
            }

            worker = PersistentWorker.create(worker_type, **worker_kwargs)
            if worker.id in self._workers:
                raise ValueError(f'Duplicated worker id: {worker.id}')

            self._workers[worker.id] = worker
            self._queues[worker.id] = queue[1]
            if worker.is_process:
                queue[0].close()
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
        for worker in self._workers.values():
            worker.close()
            if not worker.wait(timeout=timeout) and force:
                worker.terminate(force=True)

        self._workers.clear()
        self._cleaned_up = True

    def terminate(self, timeout=None):
        if self._cleaned_up:
            return

        timeout = timeout if timeout is not None else self.timeout
        for worker in self._workers.values():
            worker.terminate(timeout=timeout, force=True)

        self._workers.clear()
        self._cleaned_up = True


    def run(self, *seq_generators, results_callback=None, worker_extra_pending_inputs=0):
        if self._map_guard:
            raise RuntimeError('recursive map!')

        self._map_guard = True
        self._deplated = False
        self._pending = 0
        self._closed = set()
        self._counters = {}
        ret = []

        def next_inputs():
            if self._deplated:
                return None
            try:
                return tuple([next(gen) for gen in seq_generators])
            except StopIteration:
                self._deplated = True
                return None

        def enqueue(worker, close_if_deplated=False):
            inp = next_inputs()
            if not self._deplated:
                worker.enqueue(*inp)
                self._pending += 1
            elif close_if_deplated:
                worker.close()
                self._closed.add(worker.id)

        
        for i in range(worker_extra_pending_inputs + 1):
            for worker in self._workers.values():
                if worker.id not in self._closed:
                    enqueue(worker, close_if_deplated=not i)

        while self._pending and set(self._workers.keys()).difference(self._closed):
            ready = mp.connection.wait(list(self._queues.values()))
            for conn in ready:
                try:
                    msg = conn.recv()
                except EOFError:
                    found = False
                    for wid, queue in self._queues.items():
                        if queue is conn:
                            found = True
                            break
                    assert found
                    msg = (None, False, None, wid)
                        
                if msg is None:
                    break

                unused_counter, flag, result, wid = msg
                assert wid in self._workers
                worker = self._workers.get(wid, None)
                if not flag:
                    if not wid in self._closed:
                        # wid died
                        self._pending -= 1
                        self._closed.add(worker.id)
                    else:
                        # wid gracefully closed
                        pass

                    self._queues[worker.id].close()
                    del self._queues[worker.id]
                else:
                    assert wid not in self._closed
                    self._pending -= 1
                    if results_callback is not None:
                        result = results_callback(worker, result)
                    ret.append(result)
                    if worker.id not in self._closed:
                        enqueue(worker, close_if_deplated=True)

        return ret


    def handle_new_worker(self, worker):
        pass

