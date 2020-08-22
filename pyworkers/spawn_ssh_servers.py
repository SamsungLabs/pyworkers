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
    parser.add_argument('--signal', type=str, help='An "OK" signal will be written to this file in order to communicate that all server have been created. If an error occurs, "ERR" will be written".')
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
        try:
            for addr in args.addrs:
                print(f'Creating an ssh server at: {addr}')
                # TODO: suppress_children is actually a very misleading name... in fact, when set to True the child processes
                # won't inherit the verbosity level of the main server process, but later they can be easily configured on their
                # own and be as verbose as possible...
                server = stack.enter_context(tmp_ssh_server(host=addr, wdir=args.wdir, command=args.command, suppress_children=True))
                servers.append(server)
        except:
            if args.signal:
                with open(args.signal, 'w') as f:
                    f.write("ERR")
            raise

        if args.signal:
            with open(args.signal, 'w') as f:
                f.write('OK')

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
