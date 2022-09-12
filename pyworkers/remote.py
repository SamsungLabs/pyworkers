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
import sys
import runpy
import types
import struct
import socket
import signal
import threading
import collections.abc as cabc
import multiprocessing as mp

from . import remote_pickle
from .utils import get_hostname, foreign_raise, is_windows, get_logger, classproperty, SupportClassPropertiesMeta, Pipe, gettid, setproctitle, setthreadtitle

logger = get_logger(__name__)

default_port = 60006


class ConnectionClosedError(Exception):
    pass


def send_msg(sock, msg, comment=None):
    data = remote_pickle.dumps(msg)
    data_len = struct.pack('!I', len(data))
    logger.abusive('Sending a message: {} ({})', len(data), comment)
    try:
        sock.sendall(data_len + data)
    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError) as e:
        logger.debug('Sending failed: {}', e)
        raise ConnectionClosedError() from e


def recv_msg(sock, state_overwrites=None, comment=None):
    try:
        data_len = struct.unpack('!I', sock.recv(4))[0]
    except (BrokenPipeError, struct.error, ConnectionResetError, ConnectionAbortedError, OSError) as e:
        raise ConnectionClosedError() from e

    logger.abusive('Receiving a message: {} ({})', data_len, comment)
    data = bytes()
    try:
        while data_len:
            chunk = sock.recv(data_len)
            data_len -= len(chunk)
            data += chunk
    except (ConnectionResetError) as e:
        raise ConnectionClosedError() from e
    logger.abusive('Message received ({}), deserializing...', comment)
    msg = remote_pickle.loads(data, extra_kwargs=state_overwrites)
    return msg


def set_linger(sock, enable, timeout):
    opt_bytes = struct.pack('hh' if is_windows() else 'ii', int(enable), timeout)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, opt_bytes)


def set_keepalive(sock, enable, time=None, interval=None, probes=None):
    opt_bytes = struct.pack('h' if is_windows() else 'i', int(enable))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, opt_bytes)
    if time is not None:
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, struct.pack('h' if is_windows() else 'i', int(time)))
    if interval is not None:
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, struct.pack('h' if is_windows() else 'i', int(interval)))
    if probes is not None:
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, struct.pack('h' if is_windows() else 'i', int(probes)))


def sanitize_target_host(host):
    if host:
        if isinstance(host, str):
            if ':' in host:
                host, port = host.rsplit(':', maxsplit=1)
                ret = (socket.gethostbyname(host), int(port))
            else:
                ret = (socket.gethostbyname(host), default_port)
        else:
            if not isinstance(host, cabc.Sequence) or len(host) != 2:
                raise TypeError('Invalid type for "host" parameter: {}'.format(type(host).__name__))
            ret = (socket.gethostbyname(host[0]), host[1])
    else:
        ret = ('127.0.0.1', default_port)

    return ret


class GracefulExitError(Exception):
    pass


class RemoteWorkerMeta(remote_pickle.SupportRemoteGetStateMeta, SupportClassPropertiesMeta):
    pass


class RemoteWorker(Worker, metaclass=RemoteWorkerMeta):
    def __init__(self, *args, host=None, context=None, main_path=None, **kwargs):
        self._target_host = sanitize_target_host(host)
        if main_path is None:
            try:
                main_path = os.path.abspath(sys.modules['__main__'].__file__)
            except:
                pass

        self._startup_sync = threading.Event()
        self._remote_side = False # tells us whether the class exists on the remote end
        self._is_backend = False # True if a class is accessed from the _run_backend
        self._from_remote_parent = False # used only to detect payloads passed to __setstate__ which were received from the remote part
        self._payload = None
        self._context = context
        self._remote_dead = False
        self._reset_sigterm_hnd = False # reset SIGTERM handler in the child process
        self._main_path = main_path
        if context is not None:
            kwargs.setdefault('run', True)
        super().__init__(*args, **kwargs)
        assert not self.is_child
        assert not self.is_remote_side

    def _get_restart_args(self):
        args, kwargs = super()._get_restart_args()
        kwargs.update({ 'host': self._target_host, 'context': self._context, 'main_path': self._main_path })
        return args, kwargs

    #
    # Declare type
    #

    @classproperty
    @classmethod
    def worker_type(cls, inst=None):
        return WorkerType.REMOTE

    #
    # Implement interface
    #


    @property
    def is_child(self):
        return self._remote_side and self._is_backend

    # additional property to help distinguish between parent-side and server-side
    # parent == not self.is_child and not self.is_remote_side
    # server == not.self_is_child and self.is_remote_side
    # child == self.is_child (implies self.is_remote_side)
    @property
    def is_remote_side(self):
        return self._remote_side

    def is_alive(self):
        if self.is_child:
            return True

        if self.is_remote_side:
            if self._dead:
                return False
            ret = self._child.is_alive()
            if not ret:
                self._dead = True
            return ret
        else:
            if not self._started or self._dead:
                return False

            if self._child.is_alive():
                return True

            if not self._remote_dead:
                try:
                    send_msg(self._ctrl_sock, ('alive', tuple()), comment='ctrl: alive')
                    result = recv_msg(self._ctrl_sock, comment='ctrl: alive result')
                except ConnectionClosedError:
                    # connection closed, nothing more to do than assume the child is dead
                    # at the remote side
                    logger.details('Connection to the remote control thread is closed - assuming child dead')
                    self._remote_dead = True
                    result = False

                assert isinstance(result, bool), result

                if not result:
                    if not self._remote_dead:
                        send_msg(self._ctrl_sock, None, comment='ctrl: release')
                    logger.details('Closing frontend-side control socket')
                    self._ctrl_sock.close()
                    self._remote_dead = True
                    self._dead = True
                else:
                    return True
            else:
                # both local thread and remote process are dead
                # cache result
                self._dead = True

            return False

    def wait(self, timeout=None, remote_timeout=None):
        if (timeout is not None and timeout < 0) or (remote_timeout is not None and remote_timeout < 0):
            raise ValueError('Negative timeout')
        if timeout is not None:
            if remote_timeout is None:
                remote_timeout = timeout
            else:
                remote_timeout = min(remote_timeout, timeout)

        if self.is_child:
            raise ValueError('A worker cannot wait for itself')

        if self.is_remote_side:
            if not self.is_alive():
                return True

            self._child.join(timeout)
            alive = self._child.is_alive()
            if not alive:
                self._dead = True
            return not alive
        else:
            if not self._started or self._dead:
                return True

            if not self._remote_dead:
                logger.debug('Sending a wait message with args: {}', (remote_timeout, ))
                try:
                    send_msg(self._ctrl_sock, ('wait', (remote_timeout, )), comment='ctrl: wait')
                    result = recv_msg(self._ctrl_sock, comment='ctrl: wait result')
                    logger.debug('Remote wait result: {}', result)
                except ConnectionClosedError:
                    # connection closed, nothing more to do than assume the child is dead
                    # at the remote side
                    logger.details('Connection to the remote control thread is closed - assuming child dead')
                    self._remote_dead = True
                    result = True

                assert isinstance(result, bool), result
                if not result:
                    return False

                if not self._remote_dead:
                    send_msg(self._ctrl_sock, None, 'ctrl: release')
                logger.debug('Closing frontend-side control socket')
                self._ctrl_sock.close()
                self._remote_dead = True

            self._child.join(timeout)
            alive = self._child.is_alive()
            if not alive:
                self._dead = True
            return not alive

    def terminate(self, timeout=5, force=True, remote_timeout=1, *, _release_remote_ctrl=False):
        ''' remote_timeout will be min(remote_timeout, timeout) (with None being equivalent of inf)
        '''
        if (timeout is not None and timeout < 0) or (remote_timeout is not None and remote_timeout < 0):
            raise ValueError('Negative timeout')
        if timeout is not None:
            if remote_timeout is None:
                remote_timeout = timeout
            else:
                remote_timeout = min(remote_timeout, timeout)

        if self.is_child:
            raise WorkerTerminatedError()
        if self.is_remote_side:
            if not self.is_alive():
                return True

            self._ctrl_comms.parent_end.send('terminate')
            self._release_child()
            self._child.join(timeout)
            if self._child.is_alive():
                if force:
                    self._child.terminate()
                    self._child.join(timeout)
                    try:
                        send_msg(self._socket, (False, None), comment='data: force terminate result')
                        self._socket.close()
                    except ConnectionClosedError:
                        pass

            alive = self._child.is_alive()
            if not alive:
                self._dead = True
                self._ctrl_comms.parent_end.close()
            if not alive and _release_remote_ctrl and self._ctrl_thread_rem.is_alive():
                foreign_raise(self._ctrl_thread_rem.ident, GracefulExitError)
                try:
                    self._ctrl_sock.shutdown(socket.SHUT_RD)
                except OSError:
                    pass
                self._ctrl_thread_rem.join(timeout)

            return not alive
        else:
            if not self._started or self._dead:
                return True

            if not self._remote_dead:
                logger.debug('Sending a terminate message with args: {}', (remote_timeout, force))
                try:
                    send_msg(self._ctrl_sock, ('terminate', (remote_timeout, force)), comment='terminate')
                    #self._socket.shutdown(socket.SHUT_WR)
                    result = recv_msg(self._ctrl_sock, comment='terminate result')
                    logger.debug('Remote terminate result: {}', result)
                except ConnectionClosedError:
                    # connection closed, nothing more to do than assume the child is dead
                    # at the remote side
                    logger.debug('Connection to the remote control thread is closed - assuming child dead')
                    self._remote_dead = True
                    result = True

                #if not self._remote_dead:
                #    self._release_child()

                assert isinstance(result, bool), result
                if not result:
                    return False

                if not self._remote_dead:
                    send_msg(self._ctrl_sock, None, comment='ctrl: release')
                logger.debug('Closing frontend-side control socket')
                self._ctrl_sock.close()
                self._remote_dead = True

            self._child.join(timeout)
            if self._child.is_alive() and force:
                os.kill(os.getpid(), signal.SIGTERM)

            alive = self._child.is_alive()
            if not alive:
                self._dead = True
            return not alive

    def _get_result(self):
        return self._result

    #
    # Running mechanism
    #

    # Parent-side, calling thread
    def _start(self):
        logger.details('Connecting to {}', self._target_host)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_linger(self._socket, True, 0)
        set_keepalive(self._socket, True)
        self._socket.connect(self._target_host)
        logger.debug('Data socket at: {}', self._socket.getsockname())

        logger.debug('Spinning up a frontend thread')
        self._child = threading.Thread(target=self._run_frontend, name=f'{self.name} (remote front)')
        self._child.start()
        self._dead = False
        logger.debug('Waiting for the frontend thread to notify that everything is up and running...')
        self._startup_sync.wait()
        logger.details('Child created successfully, continuing with the main thread')

    # Parent-side, helper thread managing network communication and fetching results from the child
    def _run_frontend(self):
        logger.details('Frontend reached')
        assert os.getpid() == self._pid # the same process as parent
        assert gettid() != self._tid # different thread
        if self._set_names:
            setthreadtitle(f'{self.name} (remote front)', self)

        logger.debug('Sending self to the server to initialize backend...')
        send_msg(self._socket, (self._context, True), comment='data: header')
        send_msg(self._socket, self, comment='data: initial remote worker') # this will spawn a backend at the remote side, via __getstate__(remote=True) and __setstate__

        logger.debug('Waiting for control socket address from the child...')
        control_addr = recv_msg(self._socket, comment='control socket addr')

        logger.debug('Control socket address from the child: {}, connecting...', control_addr)
        self._ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_keepalive(self._ctrl_sock, True)
        self._ctrl_sock.connect(control_addr)
        logger.debug('Control sockets connected: {} <==> {}', self._ctrl_sock.getsockname(), control_addr)

        self._host, self._pid, self._tid, self._ident = recv_msg(self._ctrl_sock, comment='ctrl: runtime info')
        logger.debug('Received info package from the backend, signalling the main thread that everything is fine')
        self._startup_sync.set()
        self._fetch_results()
        logger.debug('Closing down frontend-side socket')
        self._socket.close()
        logger.details('Frontend thread finished')

    # Parent-side, called from frontend (controlling thread)
    # Helper function implementing results fetching mechanism
    # here simply wait for the result
    # is overwritten in the persistent remote worker class,
    # where fetching happens in the loop
    def _fetch_results(self):
        logger.details('Waiting for the result...')
        try:
            self._result = recv_msg(self._socket, comment='data: result')
            logger.debug('Result received')
        except ConnectionClosedError:
            self._result = (False, None)
            logger.debug('Connection to the child has been closed before receiving the result')
        else:
            self._user_state = recv_msg(self._socket, comment='data: user state')
            logger.debug('User state received')
        logger.details('Result: {}', self._result)

    # Handles serialization between:
    # if remote is True: frontend* (parent-side) --> server process (remote-side)
    # otherwise: server process* (remote-side) --> child process (remote-side)
    # * - we are here
    def __getstate__(self, remote=False):
        if not remote and not self._remote_side:
            return self.__dict__.copy()

        if remote:
            assert not self._remote_side
            assert not self._is_backend
            logger.debug('Serializing RemoteWorker...')
            state = self.__dict__.copy()
            state['_from_remote_parent'] = True
            state['_child'] = None
            state['_socket'] = None # _socket will be injected by the server on the remote side
            state['_startup_sync'] = None
            if self._context is None:
                state['_payload'] = remote_pickle.dumps((self._target, self._args, self._kwargs))
            del state['_target']
            del state['_args']
            del state['_kwargs']
            return state
        else:
            assert self._remote_side
            assert not self._is_backend
            state = self.__dict__.copy()
            return state

    # Handles deserialization between:
    # if _from_remote_parent is True: frontend (parent-side) --> server process* (remote-side)
    # otherwise: server process (remote side) --> child process* (remote-side)
    # * - we are here
    def __setstate__(self, state):
        self.__dict__.update(state)
        if self._from_remote_parent:
            assert self._socket is not None # should be injected to 'state' by the server
            assert not self._remote_side
            assert not self._is_backend
            assert self._payload is not None or self._context is not None

            self._from_remote_parent = False
            self._remote_side = True
            self._is_backend = False

            logger.debug('Client data socket is: {}', self._socket.getpeername())
            logger.debug('Creating a control socket for this connection...')
            self._ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._ctrl_sock.bind((self._socket.getsockname()[0], 0))
            self._ctrl_sock.listen()
            logger.debug('Control socket listening at {}', self._ctrl_sock.getsockname())

            logger.debug('Notifying the parent about newly created control socket...')
            send_msg(self._socket, self._ctrl_sock.getsockname(), comment='control socket addr')

            incoming = self._ctrl_sock
            logger.debug('Waiting for a connect to the control socket from the parent')
            self._ctrl_sock, ctrl_peer = incoming.accept()
            set_keepalive(self._ctrl_sock, True)
            logger.details('Control sockets connected: {} <==> {}', self._ctrl_sock.getsockname(), ctrl_peer)
            logger.debug('Closing listening socket')
            incoming.close()

            logger.debug('Spinning up a backend child process...')
            self._comms = Pipe()
            self._ctrl_comms = Pipe()

            # we need to be careful not to send a control socket here (see __getstate__)
            self._child = mp.get_context('spawn').Process(target=self._run_backend, name=f'{self.name}')
            self._child.start()
            self._dead = False

            # Clean up things which are only needed in the backend
            self._payload = None

            self._startup_sync = threading.Event()
            self._ctrl_thread_rem = threading.Thread(target=self._ctrl_fn_remote, name=f'{self._name} (remote control thread)')
            self._ctrl_thread_rem.start()
            self._startup_sync.wait()

            # Receiving runtime info is a signal for us that everything is ok
            runtime_info = self._comms.parent_end.recv()
            self._host, self._pid, self._tid, self._ident = runtime_info
            send_msg(self._ctrl_sock, runtime_info, comment='ctrl: runtime info')
            self._comms.parent_end.send(True)
            self._comms.parent_end.close()
        elif self._remote_side:
            assert self._remote_side
            assert not self._is_backend
        else:
            return

    # Remote-side, child process' main (working) thread
    def _run_backend(self):
        logger.details('Backend reached')
        logger.debug('Data socket is: {}', self._socket.getpeername())
        assert self._host != get_hostname() or self._pid != os.getpid()
        assert self._socket.getsockname() == self._target_host

        if self._reset_sigterm_hnd:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

        set_linger(self._socket, True, 5)

        self._is_backend = True

        logger.debug('Getting runtime info')
        self._host = get_hostname()
        self._pid = os.getpid()
        self._tid = gettid()
        self._ident = threading.get_ident()
        if self._set_names:
            setproctitle(self.name, self)

        self._aux_socket_my, self._aux_socket_ctrl = None, None

        try:
            result = None

            if is_windows():
                # Extra pair of sockets to release the backend of the persistent worker
                # hanging on `recv`, on Windows. We need that because `shutdown(SHUT_RD)`
                # does not work on Windows (like it does on Linux). See the big comment
                # in PersistentRemoteWorker's `do_work` method for more details.
                self._aux_socket_my, self._aux_socket_ctrl = socket.socketpair()

            logger.debug('Spinning up a control thread')
            self._ctrl_thread_loc = threading.Thread(target=self._ctrl_fn_local, name=f'{self.name} (local control thread)')
            self._ctrl_thread_loc.start()

            self._comms.parent_end.close()

            if self._main_path:
                logger.debug('Trying to update __main__')
                try:
                    # This follows how __mp_main__ is called in multiprocessing with "spawn"
                    # see _fixup_main_from_path in cpython's spawn.py
                    main_module = types.ModuleType("__new_main__")
                    main_content = runpy.run_path(self._main_path,
                                                    run_name="__new_main__")
                    main_module.__dict__.update(main_content)
                    sys.modules['__main__'] = sys.modules['__new_main__'] = main_module
                except:
                    logger.debug('Error occurred while trying to setup new main', exc_info=1)
                    pass

            if not hasattr(self, '_target'):
                assert self._payload is not None
                logger.debug('Deserializing payload...')
                self._target, self._args, self._kwargs = remote_pickle.loads(self._payload)
                self._payload = None
            else:
                assert self._payload is None

            try:
                assert self.is_child
                logger.debug('Sending a info package to the frontend')
                self._comms.child_end.send((self._host, self._pid, self._tid, self._ident))
                unused_sync = self._comms.child_end.recv()
                self._comms.child_end.close()

                self._init_child()

                logger.debug('Running the main function')
                result = self.do_work()
                result = (True, result)
            except Exception as e:
                logger.exception('Exception occurred while running the main function')
                result = (False, e)
            finally:
                if self._ctrl_thread_loc.is_alive():
                    logger.debug('Releasing the local control thread')
                    self._ctrl_comms.parent_end.send(None)
                    self._ctrl_thread_loc.join()
        except Exception as e:
            logger.exception('Exception occurred in the worker code')
            result = (False, e)
            self._comms.child_end.send((self._host, self._pid, self._tid, self._ident))
            unused_sync = self._comms.child_end.recv()
            self._comms.child_end.close()
            if hasattr(self, '_ctrl_thread_loc') and self._ctrl_thread_loc.is_alive():
                logger.debug('Releasing the local control thread')
                self._ctrl_comms.parent_end.send(None)
                self._ctrl_thread_loc.join()
        finally:
            self._cleanup()
            logger.debug('Sending result')
            send_msg(self._socket, result, 'data: result')
            send_msg(self._socket, self._user_state, 'data: user state')
            logger.debug('Closing down backend-side socket')
            self._socket.shutdown(socket.SHUT_WR)
            self._socket.close()
            if is_windows() and self._aux_socket_my is not None:
                self._aux_socket_my.close()

        logger.details('Backend finished')

    def _init_child(self):
        pass

    def _cleanup(self):
        pass

    # Remote-side, child process' control thread to communicate with the server process (local communication)
    def _ctrl_fn_local(self):
        logger.details('Local control thread started')
        assert self._remote_side and self._is_backend
        if self._set_names:
            setthreadtitle(f'{self.name} (local control thread)', self)
        sig = self._ctrl_comms.child_end.recv()
        try:
            if sig is not None:
                assert sig == 'terminate'
                logger.debug('Local terminate requested, raising an exception in the main thread...')
                foreign_raise(self._ident, WorkerTerminatedError)
                self._release_self()
            else:
                logger.debug('Requested to finish local control thread gracefully')
        finally:
            if is_windows():
                self._aux_socket_ctrl.close()
        logger.details('Closing local control thread')

    # Remote-side, server's process control thread handling control requests w.r.t. the child process
    # from the parent-side (remote communication)
    # This serves as an intermediate layer between the server and the parent, when it comes
    # to controlling the child process
    # A diagram of the overall communication scheme is presented below.
    # The upper communication channel is used for control messages (terminate, wait etc.).
    # The lower one is used to send arguments and results between the child and the parent directly.
    #
    #                                   Server process                  Child process
    #                               +-------------------+           +--------------------+
    # Parent ----- *network* -------+--> Remote Ctrl Th-+----+------+--> Local Ctrl Th   |
    #   |                           |                   |    |      |        \/          |
    #   |                           |      Server Th ---+----+      |      Child Th      |
    #   |                           +-------------------+           +--------/\----------+
    #   |                                                                    |
    #   +----------*network* ------------------------------------------------+
    #
    def _ctrl_fn_remote(self):
        logger.details('Remote control thread started')
        assert self._remote_side and not self._is_backend
        if self._set_names:
            setthreadtitle(f'{self.name} (remote control thread)', self)
        self._startup_sync.set()
        try:
            while True:
                ready = mp.connection.wait([self._ctrl_sock, self._child.sentinel])
                if self._child.sentinel in ready:
                    try:
                        self._socket.shutdown(socket.SHUT_WR)
                        self._socket.close()
                    except OSError:
                        pass
                    raise GracefulExitError()
                msg = recv_msg(self._ctrl_sock, comment='ctrl: generic')
                if msg is None:
                    raise GracefulExitError()

                cmd, args = msg
                logger.debug(f'Received new remote control message: {cmd}')
                if cmd == 'terminate':
                    logger.debug('Calling local terminate...')
                    result = self.terminate(*args)
                elif cmd == 'wait':
                    logger.debug('Calling local wait...')
                    result = self.wait(*args)
                elif cmd == 'alive':
                    logger.debug('Calling local alive...')
                    result = self.is_alive()
                else:
                    result = 'unknown command'

                send_msg(self._ctrl_sock, result, comment=f'ctrl: {cmd} result {result}')
        except GracefulExitError:
            logger.debug('Requested to finish remote control thread gracefully')
        finally:
            logger.debug('Closing down backend-side control socket')
            try:
                self._ctrl_sock.close()
            except OSError:
                pass

        logger.details('Closing remote control thread')
