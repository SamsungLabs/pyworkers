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

from .worker import Worker, WorkerType, WorkerTerminatedError

import os
import queue
import logging
import threading
import multiprocessing as mp

from .utils import foreign_raise, classproperty, Pipe, get_logger, gettid, setproctitle, setthreadtitle

logger = get_logger(__name__)


class ProcessWorker(Worker):
    def __init__(self, *args, **kwargs):
        self._comms = Pipe()
        self._ctrl_comms = Pipe()
        self._is_child = False
        super().__init__(*args, **kwargs)
        assert not self.is_child
        self._comms.child_end.close()
        self._ctrl_comms.child_end.close()

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
            #assert Worker.get_current_id() == self.id or Worker.get_current_id() == self.parent_id
            assert bool(self._is_child) == bool(Worker.get_current_id() == self.id)
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
            try:
                self._ctrl_comms.parent_end.put('terminate')
                self._ctrl_comms.parent_end.get()
            except (BrokenPipeError, queue.Empty):
                pass

            self._release_child()
            self._child.join(timeout)
            if self._child.is_alive():
                if force:
                    self._child.terminate()
                    self._child.join(timeout)
                    # try:
                    #     self._comms.child_end.put((False, None))
                    #     self._comms.child_end.close()
                    # except (OSError, BrokenPipeError):
                    #     pass

            alive = self._child.is_alive()
            if not alive:
                self._dead = True
                self._ctrl_comms.parent_end.close()
            return not alive

    def _get_result(self):
        if self.is_alive():
            assert self._result is None
            return None
        if self._result is None:
            #assert not self._comms[0].empty()
            #self._comms.child_end.close()
            while True:
                try:
                    self._result = self._comms.parent_end.get()
                except queue.Empty:
                    break

            if self._result is None:
                self._result = (False, None)
            else:
                self._result, self._user_state = self._result

        return self._result

    #
    # Running mechanism
    #

    # Parent-side
    def _start(self):
        self._child = mp.get_context('spawn').Process(target=self._run, name=self.name)
        self._child.start()
        self._dead = False
        ready = mp.connection.wait([self._comms.parent_end, self._child.sentinel])
        if self._comms.parent_end in ready:
            self._pid, self._tid, self._ident = self._comms.parent_end.recv()
            assert self._pid == self._child.pid
        else:
            assert self._child.sentinel in ready

    # Children-side, main (working) thread
    def _run(self):
        assert self._pid != os.getpid()
        self._is_child = True
        self._child = None
        self._pid = os.getpid()
        self._tid = gettid()
        self._ident = threading.get_ident()
        if self._set_names:
            setproctitle(self.name, self)

        self._terminate_req = False
        self._ctrl_thread_sync = threading.Event()
        self._ctrl_thread = threading.Thread(target=self._ctrl_fn, name=f'{self.name} (control thread)', daemon=True)
        self._ctrl_thread.start()
        self._ctrl_thread_sync.wait()

        self._comms.parent_end.close()
        #self._ctrl_comms.parent_end.close()

        try:
            #assert self.is_child
            self._comms.child_end.put((self._pid, self._tid, self._ident))
            self._init_child()
            result = self.do_work()
            self._comms.child_end.put(((True, result), self._user_state))
        except Exception as e:
            logger.exception('Exception occurred while running the main function')
            self._comms.child_end.put(((False, e), self._user_state))
        finally:
            self._cleanup()
            if self._ctrl_thread.is_alive() and not self._terminate_req:
                self._ctrl_comms.parent_end.send(None)
                self._ctrl_thread.join()
            self._comms.child_end.close()

    def _init_child(self):
        pass

    def _cleanup(self):
        pass

    # Children-side, control thread
    def _ctrl_fn(self):
        assert self._is_child
        if self._set_names:
            setthreadtitle(f'{self.name} (control thread)', self)
        self._ctrl_thread_sync.set()
        sig = self._ctrl_comms.child_end.recv()
        if sig is None:
            self._ctrl_comms.child_end.close()
            return

        self._terminate_req = True
        foreign_raise(self._ident, WorkerTerminatedError)
        self._release_self()
        self._ctrl_comms.child_end.close()
