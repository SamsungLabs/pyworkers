from .worker import Worker, WorkerType, WorkerTerminatedError
from .utils import foreign_raise, classproperty

import os
import signal
import threading

from .utils import get_logger

logger = get_logger(__name__)


class ThreadWorker(Worker):
    def __init__(self, target, **kwargs):
        self._startup_sync = threading.Event()
        super().__init__(target, **kwargs)
        assert not self.is_child

    #
    # Declare type
    #

    @classproperty
    @classmethod
    def worker_type(cls, inst=None):
        return WorkerType.THREAD
    
    #
    # Implement interface
    #

    @property
    def is_child(self):
        assert Worker.get_current_id() == self.id or Worker.get_current_id() == self.parent_id
        if not self._started:
            return False
        return self._tid == threading.get_ident()

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
        if self.is_child:
            raise ValueError('A worker cannot wait for itself')
        if not self.is_alive():
            return True
        self._child.join(timeout)
        alive = self._child.is_alive()
        if not alive:
            self._dead = True
        return not alive

    def terminate(self, timeout=1, force=False):
        ''' Terminate the child thread. See Worker.terminate
            for the generic description of the function's
            arguments and behaviour.
            Because there is no obvious way of force-killing
            a thread, if force is set `True` we fallback to
            force-killing it by sending a SIGTERM signal
            to the entire process. This is mostly likely
            undesired as it will kill the caller thread as well,
            therefore, unlike other workers, by default it is disabled.
        '''
        if timeout < 0:
            raise ValueError('Negative timeout')

        if not self.is_alive():
            return True

        foreign_raise(self._tid, WorkerTerminatedError)
        self._release_child()
        self._child.join(timeout)
        if self._child.is_alive():
            if force:
                os.kill(os.getpid(), signal.SIGTERM)

        alive = self._child.is_alive()
        if not alive:
            self._dead = True
        return not alive

    def _get_result(self):
        if not hasattr(self, '_result'):
            return None

        # _result is set by the child directly
        return self._result

    #
    # Running mechanism
    #

    # Parent-side
    def _start(self):
        self._child = threading.Thread(target=self._run, name=self.name)
        self._child.start()
        self._dead = False
        self._startup_sync.wait()
        assert self._tid == self._child.ident

    # Children-side
    def _run(self):
        assert self._tid != threading.get_ident()
        self._tid = threading.get_ident()
        self._startup_sync.set()
        try:
            assert self.is_child
            self._init_child()
            self._result = (True, self.do_work())
        except BaseException as e:
            logger.exception('Exception occurred while running the main function')
            self._result = (False, e)
        finally:
            self._cleanup()

    def _cleanup(self):
        pass

    def _init_child(self):
        pass