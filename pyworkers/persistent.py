from .worker import Worker, classproperty

import queue


class PersistentWorker(Worker):
    def __init__(self, target, _results_pipe, **kwargs):
        if _results_pipe is None:
            raise ValueError('_results_pipe should not be None')
        self._results_pipe = _results_pipe
        super().__init__(target, **kwargs)
        self._closed = False

    @classproperty
    @classmethod
    def is_persistent(cls, inst=None):
        return True

    @property
    def results_endpoint(self):
        ''' Returns a `utils.PipeEndpoint` object which will receive results from the
            child process.
            Consider also using `next_result`.
        '''
        if self.is_child:
            raise RuntimeError('results should only be access from the parent')
        
        return self._results_pipe.parent_end

    def close(self):
        raise NotImplementedError()

    def next_result(self, block=True, timeout=None):
        if not self.is_alive():
            ret = self.results_endpoint.get_nowait()
        else:
            ret = self.results_endpoint.get(block=block, timeout=timeout)

        unused_counter, flag, value, unused_wid = ret
        if not flag:
            raise queue.Empty
        return value

    def results_iter(self, maxitems=None):
        cnt = 0
        while maxitems is None or cnt < maxitems:
            try:
                yield self.next_result()
                cnt += 1
            except queue.Empty:
                break

    def enqueue(self, *args, **kwargs):
        raise NotImplementedError()

    def call(self, *args, **kwargs):
        self.enqueue(*args, **kwargs)
        return self.next_result()

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)

    def _init_child(self):
        self._counter = 0
        self._stop = False

    def restart(self, *args, results_pipe=None, timeout=None, **kwargs):
        self.close()
        self.wait(timeout=timeout)
        if self.is_alive():
            self.terminate(*args, timeout=timeout, **kwargs)
            if self.is_alive():
                raise RuntimeError(f'Could not stop a worker!')

        ctor_args, ctor_kwargs = self._get_restart_args()
        self.__dict__.clear()
        type(self).__init__(self, *ctor_args, results_pipe=results_pipe, **ctor_kwargs, _is_restart=True)
