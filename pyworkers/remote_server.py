import os
import queue
import signal
import socket
import struct
import logging
import traceback
import threading
import contextlib
import subprocess

from .remote import send_msg, recv_msg, set_linger, default_port, ConnectionClosedError
from .worker import WorkerTerminatedError
from .process import ProcessWorker
from .utils import foreign_raise, BraceStyleAdapter, is_windows

logger = BraceStyleAdapter(logging.getLogger(__name__))


class RemoteServer():
    def __init__(self, addr, close_on_none=False):
        self.req_addr = addr
        self.addr = None
        self.socket = None
        self.children = []
        self.closed = True
        self.close_on_none = close_on_none

        logger.info('Remote server PID: {}', os.getpid())

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

                if child is None and self.close_on_none:
                    logger.info('"None" received')
                    break

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
    def __init__(self, addr, name=None, close_on_none=False):
        self._addr = addr
        self._close_on_none = close_on_none
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
        self._server = RemoteServer(self._addr, self._close_on_none)
        self._server.open_socket()
        self._comms.child_end.send(self._server.addr)
        self._server.install_handlers()
        return self._server.run()

    def _release_self(self):
        self._server.break_accept()


def spawn_server(addr, close_on_none=False):
    return RemoteServerProcess(addr, close_on_none=close_on_none)


def run_server(addr, install_handlers=True, close_on_none=True):
    server = RemoteServer(addr, close_on_none=close_on_none)
    server.open_socket()
    if install_handlers:
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


def _spawn_ssh_server(host, user, passwd, wdir, server_port):
    full_host = host
    if user:
        full_user = user
        if passwd:
            full_user = '{}:{}'.format(user, passwd)
    
        full_host = '{}@{}'.format(full_user, host)

    host = socket.gethostbyname(host)
    server_cmd = 'python3 -m pyworkers.remote_server -vv --addr {} --close_on_none --suppress_children'.format(host)
    if server_port:
        server_cmd += ' --port {}'.format(server_port)

    if wdir:
        server_cmd = 'cd {} || exit 1; echo "OK"; {}'.format(wdir, server_cmd)

    return subprocess.Popen(['ssh', '-tt', full_host, server_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.PIPE)


@contextlib.contextmanager
def tmp_ssh_server(host, user=None, passwd=None, wdir=None, server_port=None):
    ''' Returns a context manager which returns a remote server on the specified host
        by ssh-ing into it. The server is killed when the calling threads exists the manager.

        The server is spawned by the following command line::

            ssh [user[:passwd]]@host "[cd wdir;] python3 -m pyworkers.remote_server --addr host --close_on_none [--port server_port]"

        Arguments:
            host : hostname or IP address, the created server will be listening on the same
                address as this
            user : optional username for ssh command, if not provided ssh will be called without
                the 'user@' part
            passwd : optional password to use for ssh, this can be a filename in which case the file is read
                in order to obtain password (the assumption here is that if a file with name ``passwd`` exists
                then ``passwd`` is interpreted as a filename, otherwise it's treated as a password directly)
            wdir : an optional working directory from which the remote server will be called, if not provided
                the server will be spawned from the default directory to which the user is moved when ssh-ing
            server_port : optional port number on which the server should be listening

        Returns:
            A context manager which creates a server on enter and closes it on exit.
            The context manager is functionally the same as `subprocess.Popen` class,
            although it's not derived from it.

        Raises:
            ValueError : if ``passwd`` is provided without ``user``
            RuntimeError : if the server could not be spawned
    '''

    if passwd and not user:
        raise ValueError('Password without username')

    class ssh_popen():
        def __init__(self, proc, stdout_buff=None):
            self.proc = proc
            self.buffer = stdout_buff if stdout_buff is not None else bytearray()
            self.fetched = False
            self.ctrl_c = False

            self._fetcher = None

        @property
        def stdout(self):
            if self._fetcher is None:
                self._start_fetcher()

            self._fetcher.join()
            return self.buffer.decode('utf-8')

        def _start_fetcher(self):
            if self._fetcher is not None:
                return
            self._fetcher = threading.Thread(target=self._fetch, daemon=True)
            self._fetcher.start()

        def _fetch(self):
            if self.fetched:
                return

            for line in iter(self.proc.stdout.readline, b''):
                self.buffer += line

            self.proc.stdout.close()
            self.fetched = True

        def _send_ctrl_c(self):
            if self.ctrl_c:
                return

            try:
                ssh_proc.stdin.write(b'\x03')
                ssh_proc.stdin.flush()
                ssh_proc.stdin.close()
            except (BrokenPipeError, OSError):
                pass

            self.ctrl_c = True

        def terminate(self, *args, **kwargs):
            self._send_ctrl_c()
            return self.proc.terminate(*args, **kwargs)

        def wait(self, *args, **kwargs):
            self._send_ctrl_c()
            return self.proc.wait(*args, **kwargs)

        def kill(self, *args, **kwargs):
            self._send_ctrl_c()
            return self.proc.kill(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.proc, name)

    _stdout_buff = bytearray()
    ssh_proc = _spawn_ssh_server(host, user, passwd, wdir, server_port)
    ssh_proc = ssh_popen(ssh_proc, _stdout_buff)

    try:
        server_running = False
        while ssh_proc.poll() is None:
            line = ssh_proc.proc.stdout.readline()
            _stdout_buff += line
            if line.startswith(b'Listening'):
                server_running = True
                break

        if not server_running:
            raise RuntimeError('Could not create a server process:\n' + ssh_proc.stdout)

        yield ssh_proc
        try:
            ssh_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    finally:
        ssh_proc.terminate()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--addr', '-a', default='0.0.0.0', help='Address on which the server should listen.')
    parser.add_argument('--port', '-p', type=int, default=default_port, help='Port on which the server should listen.')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='Specifies output verbosity, each appearance of this argument increases verbosity bye 1.'
        ' The default verbosity is inherited from the default of a Logger object from python\'s logging module, that is only warnings and errors should be printed.'
        ' Verbosity of 1 adds generic informations to the output and 2 enables debug output. Values above 2 do not add anything.')
    parser.add_argument('--close_on_none', action='store_true', help='Close server after receiving None')
    parser.add_argument('--suppress_children', action='store_true', help='Only enable logging for remote_server module, disable it for anything else else.')
    args = parser.parse_args()

    root_logger = logging.getLogger()
    target_logger = root_logger if not args.suppress_children else logger.logger

    ch = logging.StreamHandler()
    if args.verbose == 1:
        target_logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    elif args.verbose >= 2:
        target_logger.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)

    target_logger.addHandler(ch)

    run_server((args.addr, args.port), install_handlers=True, close_on_none=args.close_on_none)
