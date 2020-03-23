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
from .utils import foreign_raise, BraceStyleAdapter, is_windows

if __name__ == '__main__':
    logger = BraceStyleAdapter(logging.getLogger())
else:
    logger = BraceStyleAdapter(logging.getLogger(__name__))



class RemoteServer():
    def __init__(self, addr):
        self.req_addr = addr
        self.addr = None
        self.socket = None
        self.children = []
        self.closed = True

    def open_socket(self):
        if not self.closed:
            return

        logger.debug('Trying to open a socket at: {}', self.req_addr)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_linger(server, True, 0)
        server.bind(self.req_addr)
        server.listen()
        self.addr = server.getsockname()
        logger.info('Listening on {}', self.addr)
        self.socket = server
        self.closed = False

    def install_handlers(self):
        def cleanup(*args):
            for child in self.children:
                if child.is_alive():
                    os.kill(child.pid, signal.SIGTERM)

            self.children.clear()
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTERM)

        signal.signal(signal.SIGTERM, cleanup)
        #signal.signal(signal.SIGKILL, cleanup)

        if is_windows():
            def release(*args):
                logger.info('SIGINT caught')
                for thread in threading.enumerate():
                    if thread.ident != threading.get_ident():
                        foreign_raise(thread.ident, KeyboardInterrupt)
                self.break_accept()

            signal.signal(signal.SIGINT, release)

    def break_accept(self):
        logger.debug('Trying a dummy connect to the server socket at: {}', self.addr)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cli:
            if self.addr[0] == '0.0.0.0':
                cli.connect(('127.0.0.1', self.addr[1]))
            else:
                cli.connect(self.addr)
            #send_msg(cli, None, comment='no worker')
            logger.debug('Dummy connect successful')

    def run(self):
        if self.closed:
            self.open_socket()

        self.children = []
        try:
            while True:
                cli, cli_addr = self.socket.accept()
                set_linger(cli, False, 0)

                logger.info('New client: {}', cli_addr)

                logger.debug('Waiting for the RemoteWorker object...')
                try:
                    child = recv_msg(cli, { '_socket': cli, '_reset_sigterm_hnd': True }, comment='server: initial remote worker')
                except ConnectionClosedError:
                    logger.info('Client disconnected before child was successfully created')
                    continue

                logger.debug('Object received, appending to the list')
                self.children.append(child)
        except (WorkerTerminatedError, KeyboardInterrupt):
            pass
        except Exception:
            logger.exception('Error occurred in the remote server:')
            raise
        finally:
            logger.info('Closing down...')
            self.socket.close()
            #self.socket.shutdown()
            for child in self.children:
                try:
                    child.terminate(timeout=1, force=True, _release_remote_ctrl=True)
                    if child.is_alive():
                        os.kill(child.pid, signal.SIGTERM)
                except:
                    logger.exception('Exception occurred while killing a remote child:')

        logger.info('Remote server closed')
        self.closed = True


class RemoteServerProcess(ProcessWorker):
    def __init__(self, addr, name=None):
        self._addr = addr
        super().__init__(target=None, args=None, kwargs=None, name=name, run=True)

    def _start(self):
        super()._start()
        self._addr = self._comms.parent_end.recv()
        if not isinstance(self._addr[0], str):
            self._result = self._addr
            self._addr = None
            self._dead = True

    @property
    def addr(self):
        return self._addr

    def run(self):
        self._server = RemoteServer(self._addr)
        self._server.open_socket()
        self._comms.child_end.send(self._server.addr)
        self._server.install_handlers()
        return self._server.run()

    def _release_self(self):
        self._server.break_accept()


def spawn_server(addr):
    return RemoteServerProcess(addr)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', '-a', default='0.0.0.0', help='Address on which the server should listen.')
    parser.add_argument('--port', '-p', type=int, default=6006, help='Port on which the server should listen.')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='Specifies output verbosity, each appearance of this argument increases verbosity bye 1.'
        ' The default verbosity is inherited from the default of a Logger object from python\'s logging module, that is only warnings and errors should be printed.'
        ' Verbosity of 1 adds generic informations to the output and 2 enables debug output. Values above 2 do not add anything.')
    args = parser.parse_args()

    ch = logging.StreamHandler()
    if args.verbose == 1:
        logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    elif args.verbose >= 2:
        logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    logger.logger.addHandler(ch)

    server = RemoteServer((args.addr, args.port))
    server.open_socket()
    server.install_handlers()

    if is_windows():
        import threading
        t = threading.Thread(target=server.run)
        t.start()
        while t.is_alive():
            import time
            time.sleep(1)
    else:
        server.run()
