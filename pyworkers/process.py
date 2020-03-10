from .worker import Worker, WorkerType, WorkerTerminatedError

import os
import signal
import platform
import threading
import multiprocessing as mp

from .utils import foreign_raise, classproperty


class ProcessWorker(Worker):
    def __init__(self, *args, **kwargs):
        self._comms = mp.Pipe()
        self._ctrl_comms = mp.Pipe()
        self._is_child = False
        super().__init__(*args, **kwargs)
        assert not self.is_child

    #
    # Declare type
    #

    @classproperty
    @classmethod
    def worker_type(cls, inst=None):
        return WorkerType.PROCESS

    #
    # Implement interface
    #

    @property
    def is_child(self):
        if self._started:
            assert bool(self._is_child) != bool(self._child)
            assert Worker.get_current_id() == self.id or Worker.get_current_id() == self.parent_id
            assert bool(self._is_child) == bool(Worker.get_current_id() != self.parent_id)
        return self._is_child

    def is_alive(self):
        if self.is_child:
            return True
        if not self._started or self._dead:
            return False
        ret = self._child.is_alive()
        if not ret:
            self._dead = True
        return ret

    def wait(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError('Negative timeout')
        if self.is_child:
            raise ValueError('A worker cannot wait for itself')
        if not self.is_alive():
            return True
        self._child.join(timeout)
        alive = self._child.is_alive()
        if not alive:
            self._dead = True
        return not alive

    def terminate(self, timeout=1, force=True):
        ''' Default timeout is 1 sec
        '''
        if timeout is not None and timeout < 0:
            raise ValueError('Negative timeout')

        if self.is_child:
            raise WorkerTerminatedError()
        if not self.is_alive():
            return True
        else:
            self._ctrl_comms[0].send('terminate')
            #_ = self._ctrl_comms[0].recv()
            self._release_child()
            self._child.join(timeout)
            if self._child.is_alive():
                if force:
                    self._child.terminate()
                    self._child.join(timeout)
                    try:
                        self._comms[1].send((False, None))
                        self._comms[1].close()
                    except (OSError, BrokenPipeError):
                        pass

            alive = self._child.is_alive()
            if not alive:
                self._dead = True
                self._ctrl_comms[0].close()
                #self._ctrl_comms.join_thread()
            return not alive

    def _get_result(self):
        if self.is_alive():
            assert not hasattr(self, '_result')
            return None
        if not hasattr(self, '_result'):
            #assert not self._comms[0].empty()
            self._comms[1].close()
            has_result = False
            while True:
                try:
                    self._result = self._comms[0].recv()
                    has_result = True
                except EOFError:
                    break

            if not has_result:
                self._result = (False, None)

        return self._result

    #
    # Running mechanism
    #

    # Parent-side
    def _start(self):
        self._child = mp.Process(target=self._run, name=self.name)
        self._child.start()
        self._dead = False
        self._pid, self._tid = self._comms[0].recv()
        assert self._pid == self._child.pid

    # Children-side, main (working) thread
    def _run(self):
        assert self._pid != os.getpid()
        self._is_child = True
        self._child = None
        self._pid = os.getpid()
        self._tid = threading.get_ident()

        self._terminate_req = False
        self._ctrl_thread_sync = threading.Event()
        self._ctrl_thread = threading.Thread(target=self._ctrl_fn, name=f'{self.name} control thread')
        self._ctrl_thread.start()
        self._ctrl_thread_sync.wait()

        try:
            self._comms[1].send((self._pid, self._tid))
            assert self.is_child
            result = self.do_work()
            self._comms[1].send((True, result))
        except Exception as e:
            self._comms[1].send((False, e))
        finally:
            if self._ctrl_thread.is_alive() and not self._terminate_req:
                self._ctrl_comms[0].send(None)
                self._ctrl_thread.join()
            self._comms[1].close()
            #self._comms.join_thread()

    # Children-side, control thread
    def _ctrl_fn(self):
        assert self._is_child
        self._ctrl_thread_sync.set()
        sig = self._ctrl_comms[1].recv()
        if sig is None:
            self._ctrl_comms[1].close()
            #self._ctrl_comms.join_thread()
            return

        self._terminate_req = True
        foreign_raise(self._tid, WorkerTerminatedError)
        self._release_self()
        #self._ctrl_comms[1].send(True)
        self._ctrl_comms[1].close()
        #self._ctrl_comms.join_thread()
