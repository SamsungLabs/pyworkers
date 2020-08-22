from .remote import RemoteWorker, recv_msg, send_msg, ConnectionClosedError, set_linger
from .persistent import PersistentWorker

import copy
import queue
import socket
import logging
import multiprocessing as mp
import multiprocessing.connection

from .utils import is_windows, BraceStyleAdapter, LocalPipe

logger = BraceStyleAdapter(logging.getLogger(__name__))


class PersistentRemoteWorker(PersistentWorker, RemoteWorker):
    def __init__(self, target, results_pipe=None, **kwargs):
        results_pipe = results_pipe or LocalPipe()
        super().__init__(target, results_pipe, **kwargs)
        self._socket_closed = False

    #
    # Implement interface
    #

    def wait(self, *args, **kwargs):
        ''' Closes the input queue (see `close`) and waits for the underlying process
            to finish. This can potentially cause deadlock if the underlaying queues are
            full.
        '''
        if self.is_child:
            raise ValueError('A worker cannot wait for itself')
        if not self.is_remote_side:
            if not self._started or self._dead:
                return True
            self.close()
        return super().wait(*args, **kwargs)

    def close(self):
        ''' Informs the child process that no more input data is expected.
            Does not synchronize the two processes - after call to this function the
            child process might still be processing previous enqueues.
        '''
        if self.is_child:
            self._stop = True
            return
        elif self.is_remote_side:
            raise RuntimeError('close should not be called from the server process!')
        else:
            if not self.is_alive():
                return
            try:
                self._release_child()
            except ConnectionClosedError:
                pass

    def enqueue(self, *args, **kwargs):
        if self.is_remote_side:
            raise RuntimeError('Enqueuing should only be done by the owner')
        if not self.is_alive() or self._closed or self._socket_closed:
            return
        try:
            send_msg(self._socket, (args, kwargs), comment='data: new args')
        except ConnectionClosedError:
            self._socket_closed = True

    #
    # Running mechanism
    #

    # Parent side - fetch results in loop instead of just waiting for a one
    def _fetch_results(self):
        counter = 0
        last_partial_result_signalled = False
        while True:
            try:
                result = recv_msg(self._socket, comment='data: result')
            except ConnectionClosedError:
                logger.info('Connection closed by the remote peer')
                self._socket_closed = True
                self._result = (False, None)
                if not last_partial_result_signalled:
                    self._results_pipe.child_end.put((counter, False, None, self.id))
                    last_partial_result_signalled = True
                break

            if len(result) > 2:
                remote_counter, valid, value, wid = result
                if not valid:
                    logger.debug('New message signalling end of partial results')
                    self._results_pipe.child_end.put(result)
                    last_partial_result_signalled = True
                    assert remote_counter == counter
                    assert value is None
                    assert wid == self.id
                else:
                    logger.debug(f'New intermediate result received: {counter}/{remote_counter}')
                    counter += 1
                    self._results_pipe.child_end.put(result)
                    assert counter == remote_counter
                    assert wid == self.id
            else:
                assert len(result) == 2
                logger.info(f'Final result received')
                self._result = result
                break

        self._results_pipe.child_end.close()

    # Do not transfer results queue over network
    def __getstate__(self, remote=False):
        state = super().__getstate__(remote=remote)
        if not remote:
            return state

        state['_results_pipe'] = None
        return state

    # Child process, run the main loop
    def do_work(self):
        counter = 0
        self._stop = False
        try:
            while not self._stop:
                args = copy.deepcopy(self._args)
                kwargs = copy.deepcopy(self._kwargs)
                if is_windows():
                    # On Windows we have to provide an extra way of signalling
                    # the backend that no more data is expected (in case
                    # terminate comes from the server).
                    # 
                    # That is because our original mechanism of shutting down receiving from
                    # `self._socket` from the server process (`socket.shutdown(socket.SHUT_RD)`)
                    # does not break pending `recv` in here (like it does on Linux).
                    # Under normal circumstances, the parent should send us `None` to release the backend,
                    # but that does not necessarily happen if the termination request comes from the server
                    # (i.e. the server was requested to close down independently from the parent).
                    # In that case it's impossible for us to "inject" `None` from the server process to be
                    # received in this process (as any sends from the server would be send to
                    # the parent, not the child).
                    # An alternative to injecting, would be closing the connection (which actually
                    # works and the backend is released then), but that makes it impossible for
                    # the child to send over its result (i.e. termination exception) to the parent,
                    # as the connection is already closed by then.
                    # We could send a dummy result from the server process and then close
                    # the connection, but the result would be less meaningful, especially in the case
                    # when the child is not actually blocked on the `recv` when the termination
                    # request kicks in (in which case we don't have any of these problems).
                    # 
                    # Because of all these, we really do need an extra channel to
                    # release the backend from the server (on Windows). We do that by an extra
                    # auxiliary socket created in `_run_backend` method of `RemoteWorker` using
                    # Python's `socket.socketpair`. These sockets only exist in the child process,
                    # and are ONLY used to transmit dummy data from the local control thread to the
                    # child, when the local control thread receives 'terminate' message from the
                    # server process. We don't care about the actual message being send, it's only
                    # used to signal the backend and wake it up. We don't even actually read it, the fact
                    # that some data is available to read is enough.
                    mp.connection.wait([self._socket, self._aux_socket_my])

                try:
                    extra = recv_msg(self._socket, comment='data: new args')
                except ConnectionClosedError:
                    # this close does not necessarily mean that the entire connection was closed,
                    # under normal circumstances, this is caused by `shutdown(socket.SHUT_RD)` called
                    # from the remote control thread on `terminate`. We get connection closed here,
                    # but we are still able to send anything needed.
                    logger.debug('Connection to the parent closed')
                    break
                if extra is None:
                    logger.debug('Backend released gracefully via a None message')
                    break
                extra_args, extra_kwargs = extra
                args[0:len(extra_args)] = extra_args
                kwargs.update(extra_kwargs)
                result = self.run(*args, **kwargs)
                counter += 1
                send_msg(self._socket, (counter, True, result, self.id), comment=f'data: partial result {counter}')
        finally:
            try:
                send_msg(self._socket, (counter, False, None, self.id))
            except ConnectionClosedError:
                pass

        return counter

    # Parent side, called to inform the child that no more data is expected
    # (used for wait, close, and terminate)
    def _release_child(self):
        if self.is_child:
            self._stop = True
            return
        elif self.is_remote_side:
            if not is_windows():
                try:
                    self._socket.shutdown(socket.SHUT_RD)
                except OSError:
                    pass
        else:
            if self._closed or self._socket_closed:
                return
            try:
                send_msg(self._socket, None, comment='data: release')
            except ConnectionClosedError:
                self._socket_closed = True
            self._closed = True

    def _release_self(self):
        if is_windows():
            # This is required because on Windows `shutdown(socket.SHUT_RD)` from the server (see _release_child)
            # does not break the pending `recv`/`wait` in the backend (for whatever reason).
            # See the big comment in `do_work` for more details
            send_msg(self._aux_socket_ctrl, None, comment='local release')
