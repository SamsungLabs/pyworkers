from .process import ProcessWorker
from .persistent import PersistentWorker

import copy
import queue
import multiprocessing as mp


class PersistentProcessWorker(PersistentWorker, ProcessWorker):
    def __init__(self, target, results_queue=None, **kwargs):
        results_queue = results_queue or mp.Queue()
        self._args_queue = mp.Pipe()
        super().__init__(target, results_queue, **kwargs)

    #
    # Implement interface
    #

    def wait(self, timeout=None):
        ''' Closes the input queue (see `close`) and waits for the underlying process
            to finish. This can potentially cause deadlock if the underlaying queues are
            full.
        '''
        if self.is_child:
            raise ValueError('A worker cannot wait for itself')
        if not self.is_alive():
            return True
        self.close()
        self._child.join(timeout)
        alive = self._child.is_alive()
        if not alive:
            self._dead = True
        return not alive

    def close(self):
        ''' Informs the child process that no more input data is expected.
            Does not synchronize the two processes - after call to this function the
            child process might still be processing previous enqueues.
        '''
        if self.is_child:
            self._stop = True
            return
        else:
            #if not self.is_alive():
            #    return
            self._release_child()

    def enqueue(self, *args, **kwargs):
        if not self.is_alive() or self._closed:
            return
        self._args_queue[0].send((args, kwargs))

    #
    # Running mechanism
    #

    # Child side
    def do_work(self):
        counter = 0
        self._stop = False
        try:
            while not self._stop:
                args = copy.deepcopy(self._args)
                kwargs = copy.deepcopy(self._kwargs)
                try:
                    extra = self._args_queue[1].recv()
                except EOFError:
                    break
                if extra is None:
                    break
                extra_args, extra_kwargs = extra
                args[0:len(extra_args)] = extra_args
                kwargs.update(extra_kwargs)
                result = self.run(*args, **kwargs)
                counter += 1
                self._results_queue.put((counter, True, result, self.id))
        finally:
            self._results_queue.put((counter, False, None, self.id))
            self._results_queue.close()
            #self._results_queue.join_thread()
            self._args_queue[1].close()
            #self._args_queue.cancel_join_thread()

        return counter

    # Parent side
    def _release_child(self):
        if self._closed:
            return
        try:
            self._args_queue[0].send(None)
        except OSError:
            pass
        self._args_queue[0].close()
        #self._args_queue.join_thread()
        self._closed = True