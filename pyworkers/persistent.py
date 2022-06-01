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

from .worker import Worker, classproperty

import queue


class WorkerClosedError(RuntimeError):
    def __init__(self, worker):
        super().__init__(f'Trying to enqueue to a closed worker: {worker}')


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
        if not self.wait(timeout=timeout):
            self.terminate(*args, **kwargs)
            if self.is_alive():
                raise RuntimeError(f'Could not stop a worker!')

        self._get_result() # this is required to sync user state in some cases (fetch results, at least persistant process)
        ctor_args, ctor_kwargs = self._get_restart_args()
        self.__dict__.clear()
        type(self).__init__(self, *ctor_args, results_pipe=results_pipe, **ctor_kwargs, _is_restart=True)
