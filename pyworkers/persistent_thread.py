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

from .thread import ThreadWorker
from .persistent import PersistentWorker, WorkerClosedError
from .utils import LocalPipe, get_logger

import copy

logger = get_logger(__name__)


class PersistentThreadWorker(PersistentWorker, ThreadWorker):
    def __init__(self, target, results_pipe=None, **kwargs):
        results_pipe = results_pipe or LocalPipe()
        self._args_pipe = LocalPipe()
        self._cleaned_up = False
        super().__init__(target, results_pipe, **kwargs)

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
            if not self.is_alive():
                return
            self._release_child()

    def enqueue(self, *args, **kwargs):
        if not self.is_alive() or self._closed:
            raise WorkerClosedError(self)
        self._args_pipe.parent_end.put((args, kwargs))

    #
    # Running mechanism
    #

    # Child side
    def do_work(self):
        while not self._stop:
            args = copy.deepcopy(self._args)
            kwargs = copy.deepcopy(self._kwargs)
            extra = self._args_pipe.child_end.get()
            if extra is None:
                break
            extra_args, extra_kwargs = extra
            args[0:len(extra_args)] = extra_args
            kwargs.update(extra_kwargs)
            result = self.run(*args, **kwargs)
            self._send_result(result)

        return self._counter

    def _send_result(self, result):
        self._counter += 1
        self._results_pipe.child_end.put((self._counter, True, result, self.id))

    def _cleanup(self):
        if self._cleaned_up:
            return

        self._results_pipe.child_end.put((self._counter, False, None, self.id))
        if hasattr(self._results_pipe.child_end, 'close'):
            logger.debug('Closing child\'s pipe end')
            self._results_pipe.child_end.close()

        self._cleaned_up = True

    # Parent side
    def _release_child(self):
        if self._closed:
            return
        self._args_pipe.parent_end.put(None)
        self._closed = True
