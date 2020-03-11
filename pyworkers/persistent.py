from .worker import Worker, WorkerType, classproperty

import queue


class PersistentWorker(Worker):
    def __init__(self, target, results_queue, **kwargs):
        if results_queue is None:
            raise ValueError('results_queue should not be None')
        self._results_queue = results_queue
        super().__init__(target, **kwargs)
        self._closed = False

    @classproperty
    @classmethod
    def is_persistent(cls, inst=None):
        return True

    @property
    def results_queue(self):
        ''' Returns a `multiprocessing.Queue` object which will receive results from the
            child process.
            Consider also using `next_result`.
        '''
        if self.is_child:
            raise RuntimeError('results should only be access from the parent')
        
        return self._results_queue

    def close(self):
        raise NotImplementedError()

    def next_result(self, block=True, timeout=None):
        if not self.is_alive():
            ret = self.results_queue.get_nowait()
        else:
            try:
                ret = self.results_queue.get(block=block, timeout=timeout)
            except BrokenPipeError:
                raise queue.Empty

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
