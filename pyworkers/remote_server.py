import os
import signal
import socket
import struct
import logging
import traceback
import threading

from .remote import send_msg, recv_msg, set_linger, ConnectionClosedError
from .worker import WorkerTerminatedError
from .process import ProcessWorker
from .utils import foreign_raise, BraceStyleAdapter

if __name__ == '__main__':
    logger = BraceStyleAdapter(logging.getLogger())
else:
    logger = BraceStyleAdapter(logging.getLogger(__name__))


def open_socket(addr, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    set_linger(server, True, 0)
    server.bind((addr, port))
    server.listen()
    info = server.getsockname()
    logger.info('Listening on {}', info)
    return server


def run_server(addr, port, socket=None):
    if socket is None:
        socket = open_socket(addr, port)

    children = []
    def cleanup(*args):
        for child in children:
            if child.is_alive():
                os.kill(child.pid, signal.SIGTERM)

        children.clear()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGTERM)

    signal.signal(signal.SIGTERM, cleanup)
    #signal.signal(signal.SIGKILL, cleanup)

    try:
        while True:
            cli, cli_addr = socket.accept()
            set_linger(cli, False, 0)

            logger.info('New client: {}', cli_addr)

            logger.debug('Waiting for the RemoteWorker object...')
            try:
                child = recv_msg(cli, { '_socket': cli, '_reset_sigterm_hnd': True }, comment='server: initial remote worker')
            except ConnectionClosedError:
                logger.info('Client disconnected before child was successfully created')

            logger.debug('Object received, appending to the list')
            children.append(child)
    except (WorkerTerminatedError, KeyboardInterrupt):
        pass
    except Exception:
        logger.exception('Error occurred in the remote server:')
        raise
    finally:
        logger.info('Closing down...')
        socket.close()
        #socket.shutdown()
        for child in children:
            try:
                child.terminate(timeout=1, force=True, _release_remote_ctrl=True)
                if child.is_alive():
                    os.kill(child.pid, signal.SIGTERM)
            except:
                logger.exception('Exception occurred while killing a remote child:')

    logger.info('Remote server closed')


class RemoteServer(ProcessWorker):
    def __init__(self, addr, port, name=None):
        self._addr = addr
        self._port = port
        super().__init__(target=None, args=None, kwargs=None, name=name, run=True)

    def _start(self):
        super()._start()
        self._addr, self._port = self._comms[0].recv()
        if not isinstance(self._addr, str):
            self._result = self._addr, self._port
            self._addr, self._port = None, None
            self._dead = True

    @property
    def addr(self):
        return self._addr

    @property
    def port(self):
        return self._port

    def run(self):
        self._server = open_socket(self.addr, self.port)
        info = self._server.getsockname()
        self._comms[1].send(info)
        return run_server(self.addr, self.port, self._server)

    def _release_self(self):
        logger.debug('Trying a dummy connect to the server socket')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cli:
           cli.connect((self._addr, self._port))
           #send_msg(cli, None, comment='no worker')
           logger.debug('Dummy connect successful')
        return


def spawn_server(addr, port):
    return RemoteServer(addr, port)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', default='0.0.0.0', help='Address on which the server should listen.')
    parser.add_argument('--port', type=int, default=6006, help='Port on which the server should listen.')
    args = parser.parse_args()
    run_server(addr=args.addr, port=args.port)
