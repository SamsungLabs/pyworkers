import os
import copy
import signal
import socket
import logging
import contextlib

from .persistent_process import PersistentProcessWorker
from .persistent_remote import PersistentRemoteWorker
from .worker import autoclose_active_children, Worker
from .remote_pickle import loads, dumps, SupportRemoteGetState
from .remote import sanitize_target_host, send_msg, recv_msg, ConnectionClosedError
from .utils import BraceStyleAdapter

logger = BraceStyleAdapter(logging.getLogger(__name__))


class RemoteContextWorker(PersistentProcessWorker):
    def __init__(self, context):
        self._children = []
        super().__init__(context._create_worker, name=f'Context {context._id}')

    def do_work(self):
        self._target(None, _check_payload=True)
        try:
            ret = super().do_work()
        finally:
            self._target(None, _clean=True)

        return ret

class RemoteContext(SupportRemoteGetState):
    def __init__(self, ctx_id, host=None, target=None, args=None, kwargs=None, extra_state=None):
        args = args if args is not None else []
        kwargs = kwargs if kwargs is not None else {}
        extra_state = extra_state if extra_state is not None else {}

        self._id = ctx_id
        self._target_host = sanitize_target_host(host)

        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._extra_state = extra_state
        self._payload = None

        self._remote = False
        self._from_remote = False
        self._alive = False
        self._worker = None
        self._children = []

        with self._connect() as s:
            send_msg(s, (self._id, False), comment='context: create header')
            send_msg(s, self, comment='context: new context')
            result = recv_msg(s, comment='context: create result')
            if not result:
                raise ValueError(f'Context with id {self._id} already exists on the target host {self._target_host!r}')

        self._alive = True

    @property
    def context_id(self):
        return self._id

    def is_alive(self):
        return self._alive

    @property
    def host(self):
        return self._target_host

    @contextlib.contextmanager
    def _connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(self._target_host)
            yield s
        finally:
            s.close()

    def _try_del(self):
        if not self._alive:
            return True

        with self._connect() as s:
            send_msg(s, (self._id, False), comment='context: del header')
            send_msg(s, None, comment='context: del request')
            result = recv_msg(s, comment='context: del result')
            self._alive = not result

        return not self._alive

    def close(self):
        if not self._remote:
            self._try_del()
        else:
            return self._worker.close()

    def wait(self, *args, **kwargs):
        if not self._remote:
            return self._try_del()
        else:
            return self._worker.wait(*args, **kwargs)

    def terminate(self, *args, _release_remote_ctrl=False, **kwargs):
        del _release_remote_ctrl # unused
        if not self._remote:
            return self._try_del()
        else:
            return self._worker.terminate(*args, **kwargs)

    def call(self, *args, **kwargs):
        if not self._remote:
            raise ValueError('Can only be called on the remote side')

        return self._worker.call(*args, **kwargs)


    def __getstate__(self, remote=False):
        state = copy.copy(self.__dict__)
        if remote:
            state['_remote'] = True
            state['_from_remote'] = True
            state['_payload'] = dumps((self._target, self._args, self._kwargs, self._extra_state))
        else:
            state['_from_remote'] = False

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self._from_remote:
            assert self._remote
            self._worker = RemoteContextWorker(self)
            self._payload = None

    def _create_worker(self, cli, _check_payload=False, _clean=False):
        if _check_payload:
            assert cli is None
            assert self._payload is not None
            self._target, self._args, self._kwargs, self._extra_state = loads(self._payload)
            self._payload = None
            return True

        if _clean:
            for child in self._children:
                try:
                    child.terminate(timeout=1, force=True, _release_remote_ctrl=True)
                    if child.is_alive():
                        os.kill(child.pid, signal.SIGTERM)
                except:
                    logger.exception('Exception occurred while killing a remote child:')

            return True

        try:
            state_patches = {
                '_socket': cli,
                '_reset_sigterm_hnd': True,
                '_target': self._target,
                '_args': self._args,
                '_kwargs': self._kwargs,
                **self._extra_state
            }
            child = recv_msg(cli, state_patches, comment='context: remote worker')
            self._children.append(child)
            return True
        except ConnectionClosedError:
            return False
