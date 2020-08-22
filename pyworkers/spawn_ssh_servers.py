import sys
import time
import signal
import argparse
import contextlib

from .remote_server import tmp_ssh_server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('addrs', nargs='+')
    parser.add_argument('--command', default=None, type=str, help='Command to run on each host')
    parser.add_argument('--wdir', default=None, type=str, help='Working directory of the servers')
    parser.add_argument('--forward_stdout', action='store_true', help='Print stdout of the server to this process stdout')
    args = parser.parse_args()

    stack = contextlib.ExitStack()
    _run = True

    def trap(*args):
        nonlocal _run
        print('Signal caught')
        _run = False

    signal.signal(signal.SIGTERM, trap)
    signal.signal(signal.SIGINT, trap)
    servers = []
    with stack:
        for addr in args.addrs:
            print(f'Creating an ssh server at: {addr}')
            # TODO: suppress_children is actually a very misleading name... in fact, when set to True the child processes
            # won't inherit the verbosity level of the main server process, but later they can be easily configured on their
            # own and be as verbose as possible...
            server = stack.enter_context(tmp_ssh_server(host=addr, wdir=args.wdir, command=args.command, suppress_children=True))
            servers.append(server)

        while _run:
            if args.forward_stdout:
                for server in servers:
                    if server.buffer:
                        print(server.buffer.decode('utf-8'), end='')
                        server.buffer.clear()

            sys.stdout.flush()
            time.sleep(1)

        print('Closing down!')

    print('End')


if __name__ == '__main__':
    main()
