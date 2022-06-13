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

import os
import copy
import threading
import contextlib
from enum import Enum

from .utils import get_hostname, classproperty, gettid, SupportClassPropertiesMeta


class WorkerType(Enum):
    THREAD = 0
    PROCESS = 1
    REMOTE = 2


class WorkerTerminatedError(Exception):
    def __init__(self, msg=None):
        msg = msg or 'terminate called'
        super().__init__(msg)


class Worker(metaclass=SupportClassPropertiesMeta):
    _active_children = []
    _children_lock = threading.Lock()

    def __init__(self, target, *, host=None, args=None, kwargs=None, name=None, userid=None, run=None, set_names=True, init_state=None, _is_restart=False):
        ''' Constructor of the base :py:class:`Worker` class. Should be called by derived classes.

            Each worker can either be run, or assumed dead immediately up-front, in which
            case creating a worker object doesn't have any side effects (like creating a thread).
            The latter use-case is implemented mostly for tests and as an optimization in cases
            ``target`` happen to be ``None`` (so there's nothing to call == nothing to do).
            By default, the behaviour is to run the worker if ``target`` is not ``None`` and
            otherwise skip running. However, in some cases it might be desired to run the worker
            regardless of ``target`` being ``None`` - with deriving from Worker and implementing target
            function directly in `run` instead of passing it via ``target`` here. In those cases,
            creation of the worker can be forced by setting ``run`` argument to ``True``. Analogically,
            it can be set to False to forcibly skip running of the worker even if ``target`` is provided.
            The default value for ``run`` is None meaning to determine whether to run the worker based
            on ``target`` - as mentioned earlier.

            A worker which is not run - either because ``target is None and run is None`` or ``run is False`` -
            will be assumed dead as soon as the object is created (i.e. is_alive() should return False, all waiting
            function should not block, etc.). Its result is set to ``has_error = False, result = None, error = None``.

            Arguments:
                target : function to call or ``None``.
                args : default arguments to pass to ``target``
                kwargs : default keyword arguments to pass to ``target``
                name : user defined name of the worker
                userid : user defined identifier of the worker
                run : a flag telling whether to actually run the worker
                set_names : whether to change process and thread names in the OS, requires "setproctitle" package (not installed by default),
                    fails silently if the package is not installed or an error occurres while trying to set a name; see documentation of "setproctitle"
                    for more details about how the desired effect is achieved.
        '''
        if target is not None and target is not False and not callable(target):
            raise ValueError('Target not callable')

        if run is None:
            run = bool(target)

        self._result = None
        self._target = target
        self._args = args or []
        self._kwargs = kwargs or {}

        self._name = name
        self._userid = userid
        self._parent_host, self._parent_pid, self._parent_tid = Worker.get_current_id()
        self._host, self._pid, self._tid = Worker.get_current_id()
        self._ident = threading.get_ident()
        self._do_run = run
        self._set_names = set_names
        self._user_state = init_state
        if run:
            self._started = True
            self._dead = True # should be set to False by the derived class, after a child is actually created
            self._start()
            if not self._dead and not _is_restart:
                Worker.register_child(self)
        else:
            self._started = False
            self._result = (True, None)

    def _get_restart_args(self):
        return [self._target], { 'args': self._args, 'kwargs': self._kwargs, 'name': self._name, 'userid': self._userid, 'run': self._do_run, 'set_names': self._set_names, 'init_state': self._user_state }

    def __repr__(self):
        return f'{type(self).__name__}(name: {self.name!r}, userid: {self.userid}, id: {self.id}, target: {self._target})'

    @staticmethod
    def active_children():
        with Worker._children_lock:
            Worker._children = [child for child in Worker._active_children if child.is_alive()]
            cpy = copy.copy(Worker._active_children)
        for child in cpy:
            yield child

    @staticmethod
    def register_child(child):
        with Worker._children_lock:
            Worker._active_children.append(child)

    @classmethod
    def create(cls, worker_type, *args, **kwargs):
        if not isinstance(worker_type, WorkerType):
            raise TypeError('Expected WorkerType value')

        mod_name = worker_type.name.lower()
        cls_name = mod_name[0].upper() + mod_name[1:] + 'Worker'
        if cls.is_persistent:
            mod_name = 'persistent_{}'.format(mod_name)
            cls_name = 'Persistent{}'.format(cls_name)

        import importlib
        mod = importlib.import_module('.' + mod_name, package=__name__.rsplit('.', maxsplit=1)[0])
        target_cls = getattr(mod, cls_name)
        return target_cls(*args, **kwargs)

    @staticmethod
    def get_current_id():
        ''' Return a tuple (hostname, process id, thread id) for the calling thread.
        '''
        return (get_hostname(), os.getpid(), gettid())

    @property
    def name(self):
        ''' Name of the worker, as set via the constructor.
        '''
        return self._name

    @property
    def userid(self):
        ''' User-provied (see constructor) identifier of the worker - can be used alongside `name` to identify workers
        '''
        return self._userid

    @property
    def host(self):
        ''' Name of the host on which the child lives.
        '''
        return self._host

    @property
    def pid(self):
        ''' Process id of the child.
        '''
        return self._pid

    @property
    def tid(self):
        ''' Thread id of the child
        '''
        return self._tid

    @property
    def id(self):
        ''' Worker's identifier, a tuple `(host, pid, tid)`
        '''
        return (self.host, self.pid, self.tid)

    @property
    def parent_host(self):
        ''' Name of the host on which the parent lives.
        '''
        return self._parent_host

    @property
    def parent_pid(self):
        ''' Process id of the parent.
        '''
        return self._parent_pid

    @property
    def parent_tid(self):
        ''' Thread id of the parent.
        '''
        return self._parent_tid

    @property
    def parent_id(self):
        ''' Identifier of the parent, a tuple `(parent_host, parent_pid, parent_tid)`
        '''
        return (self.parent_host, self.parent_pid, self.parent_tid)

    @classproperty
    @classmethod
    def is_thread(cls, inst=None):
        ''' *(classproperty)* `True` if the worker is a thread worker.
        '''
        if cls.worker_type != WorkerType.THREAD:
            if inst is not None:
                assert cls.is_remote or cls.is_process or inst._tid == inst._parent_tid
            return False
        return True

    @classproperty
    @classmethod
    def is_process(cls, inst=None):
        ''' *(classproperty)* `True` if the worker is a process worker.
        '''
        if cls.worker_type != WorkerType.PROCESS:
            if inst is not None:
                assert cls.is_remote or inst._pid == inst._parent_pid
            return False
        return True

    @classproperty
    @classmethod
    def is_remote(cls, inst=None):
        ''' *(classproperty)* `True` if the worker is a remote worker.
        '''
        if cls.worker_type != WorkerType.REMOTE:
            if inst is not None:
                assert inst._host == inst._parent_host and (inst._pid != inst._parent_pid or inst._tid != inst._parent_tid)
            return False
        return True

    @classproperty
    @classmethod
    def is_persistent(cls, inst=None):
        ''' *(classproperty)* Return True if the worker is persistent (see PersistentWorker).
            Otherwise return False.
        '''
        return False

    @property
    def result(self):
        ''' Return result of the :py:meth:`do_work` function, if it has been executed gracefully.
            Otherwise (worker still running or died due to an exception) return None.
            This function does not synchronize the caller with the worker - use `wait` to do that.
            In case the :py:meth:`do_work` function can return None, use `has_error` to determine whether the
            function has finished gracefully or not.
        '''
        r = self._get_result()
        if r is None:
            return None

        graceful, result = r
        if not graceful:
            return None
        return result

    @property
    def error(self):
        ''' Return unhandled exception which occurred in the worker, provided any such exception has been raised.
            Otherwise (worker still running or has finished gracefully) return None.
            This function does not synchronize the caller with the worker - use `wait` to do that.
            To check if an exception has occurred, use `has_error` as an alternative to checking for this field being None.
        '''
        r = self._get_result()
        if r is None:
            return None

        graceful, result = r
        if graceful:
            return None
        return result

    @property
    def has_error(self):
        ''' Return True if the worker finished due to an error and didn't have a chance to send its final result
            (in which case `result` will return None and `error` will hold the exception which caused the worker
            to die). Otherwise, return False if the worker finished gracefully - in which case `result` will hold
            the value returned by the `do_work` function and `error' will be set to None.
            Return None if the worker is still alive.
            This function does not synchronize the caller with the worker - use `wait` to do that.
        '''
        r = self._get_result()
        if r is None:
            return None

        graceful, _ = r
        return not graceful

    @property
    def user_state(self):
        ''' User-defined state of the worker. The special thing about this state is that it is synchronized between
            parent and child. The synchronization occurres at the end of the child's life - basically before the worker
            closes down, its parent will receive the last value of this property and will update the related object
            on its side, so its ``user_state`` has the same value as the last value observed on the child's side.
            This state can then be obtained by the user and, for example, passed as an initial state for a different worker
            (see ``init_state`` constructor argument).

            > **Note**: Changes to the user state on the parent side are prohibited, since they will never be observed on the child's side.
            > You should use ``init_state`` constructor argument to specify initial value of the property that will be sent to the child.
            > After the construction of the object finishes, there is no way to update the child's copy of this property from the parent.

            > **Note:** Changes to this property made on the child's side become visible on the parent side only after the child has finished,
            > and only by the means of the last value. Especially in the case of persistant workers, if you want to notify the parent about some
            > things changing on the worker's side while the worker is still working, you should consider using a different mechanism (e.g., wrap your
            > workload function so that the interesting changes are returned together with its normal output).

            > **Warning**: thread workers are a natural exception to the rules above as they share memory with the parent thread. Currently the
            > implementation does not use any mechanism to separate the child's and the parent's copies and thus enforce consistent behaviour with
            > other worker types in that aspect. Therefore, while the child is still running, user should not assume neither that the changes will be visible
            > nor that they won't. This might change in the future, so that the behaviour is consistent at least in the case of ``user_state``,
            > if proven beneficial.
        '''
        return self._user_state

    @user_state.setter
    def user_state(self, value):
        if not self.is_child:
            raise RuntimeError('user_state can only be modified from within the worker')
        self._user_state = value

    def close(self):
        ''' Inform the worker that no more data will be send from the parent.
            Only meaningful for certain subclasses (e.g. persistent workers).
            This function does not synchronize with the worker, simply informs it that it can
            close down after processing of all pending data is completed.
            To synchronize, please use `wait`.
        '''
        pass

    #
    # The following functions should be implemented in the derived class
    #

    @classproperty
    @classmethod
    def worker_type(cls, inst=None):
        ''' *(classproperty)* Return the worker's type (see WorkerType enum).
            This only differentiate between thread, process and remote workers and
            does not provide any information about their subclasses.
            To check if the worker is persistent, please use `is_persistent`.
            For non-standard subclasses, if a a custom mechanism should be implemented
            for 
        '''
        raise NotImplementedError()

    @property
    def is_child(self):
        ''' Return True if the worker object lives on the child-side (only guaranteed to be correct when
            called from either the parent or the child, accessing from any auxiliary thread/process might
            result in wrong values - if the object can live in more than the parent and the child and more
            fain-grained differentiation is needed, one should have its own ways of checking that).
        '''
        raise NotImplementedError()

    def is_alive(self):
        ''' Return True if (to the best of our knowledge) the worker is still alive.
            The function is conservative when it comes to proclaiming a worker dead, that is it will rather say it is
            alive than dead. Because of that it is possible that the final result might already be available even though
            `is_alive()` would still return True - that is because the worker is still considered to be alive when cleaning up.
        '''
        raise NotImplementedError()

    def wait(self, timeout=None):
        ''' Wait until the worker is dead (equivalent to `not is_alive()`) or `timeout` seconds has reached (None meaning infinite time).
            Return True if the worker is dead when the function ends, otherwise False.
            This function calls `close` before waiting for the worker.
        '''
        raise NotImplementedError()

    def terminate(self, timeout=1, force=True):
        ''' Requests the worker to end its computations abnormally.
            Under normal circumstances a call to `terminate` will first try to raise
            a `WorkerTerminatedError` exception in the child's thread (possibly crossing
            process and/or host boundaries along the way) to allow it to finish gracefully.
            The `timeout` argument controlls how much time (in seconds) the child is given to react to the
            termination request, measured from the moment an exception is raised.
            If the child is still alive after that and `force` is set to True, the function
            will then try to force-kill the child with the specific methodology
            being dependent on the worker type.
            No matter what are the arguments' values, the function always returns True if the child
            is dead at the end of the function, and False otherwise. The important part here is:
            'at the end of the function` which means that in some cases, the function might return
            'False' signalling that the child was unsuccessfully terminated, but in fact the child
            is still cleaning up and will die shortly after. This can most likely happen if the function
            is called with `timeout=0, force=False` as the child process will not have enough time to react
            to the termination request but might fulfil it at some point in the future.
        '''
        raise NotImplementedError()

    def _get_result(self):
        ''' Called by the base class to check if the (final) result is available.
            The returned value should be None if the result is not yet available,
            or a tuple `(bool, result_or_exception)` where the first value indicates if the target
            function finished calculations gracefully, in which case the second value
            is its returned value; or whether an exception occurred at any point before/after/during
            execution of the target function, in which case the second value will hold the
            exception's value.

            The check should be non-blocking and is allowed to return a non-None value even if
            the child process is still alive, as long as the correct value is returned
            (this can happen if the result has already been transferred to the parent
            but the child is still cleaning up).
            In other words, if the result is available, no synchronization between parent
            and children is guaranteed,  except for the fact that the child has at least
            reached the point where the final result is transferred.
        '''
        raise NotImplementedError()

    def _start(self):
        ''' Implement mechanism to start the child.
            The function should synchronize with the child up to the point
            where runtime information (process id, etc.) about the child is passed
            to the parent. In other words, after _start finished, the state of the
            object in the parent should be consistent with the state of the object
            in the child. Please note that "consistent" here does not necessarily mean
            "exactly the same" as some values might be expected to be different when
            accessed from different sides. However, if there is a relation which
            should hold between any of them, any guarantees are only given after `_start`
            finishes, and the object can potentially be in an undefined/invalid
            state for either the entire duration of `_start` or some of its parts.
        '''
        raise NotImplementedError()

    #
    # The following functions are likely subjects of overwriting in derived classes
    # they are here only for that purpose (to allow customization at different moments/places)
    #

    def run(self, *args, **kwargs):
        ''' Run the target function once with the given arguments.
        '''
        if self._target is None:
            return
        return self._target(*args, **kwargs)

    def do_work(self):
        ''' Provides extra level of encapsulation of calling '_target" for persistent workers.
            Persistent workers provide extra layer of control flow which calls 'self._target' multiple times.
            When the persistency logic is implemented by overwriting `self.run`, then it's impossible
            to derive from such a class and customize 'self.run' without breaking it. Therefore the current convention
            is to have `self.run` call `_target` only once while `self.do_work` can be used to implement more complex control
            flow, which can potentially call `self.run` multiple times during the live of the worker.
            For standard workers, this simply calls `self.run` once.
        '''
        return self.run(*self._args, **self._kwargs)

    def _release_child(self):
        ''' Called from the parent process (main thread) when terminate is requested,
            after sending a relevant signal to the child process.
            Provides a way for a derived class to customise actions which should happen
            on the parent side in order for the child process to react on termination request.
        '''
        pass

    def _release_self(self):
        ''' Called from the child process (control thread) when terminate is requested.
            Provides a way for a derived class to customise actions which should happen
            on the child side in order for the working thread to react on termination request.
            This function is never called for thread workers, as the child process doesn't exist.
        '''
        pass


@contextlib.contextmanager
def autoclose_active_children(timeout=0.1):
    try:
        yield
    finally:
        for child in Worker.active_children():
            child.close()
            if not child.wait(timeout=0.1):
                child.terminate(timeout=0.1)
