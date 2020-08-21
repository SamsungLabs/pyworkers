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
    args = parser.parse_args()

    stack = contextlib.ExitStack()
    _run = True

    def trap(*args):
        nonlocal _run
        print('Signal caught')
        _run = False

    signal.signal(signal.SIGTERM, trap)
    signal.signal(signal.SIGINT, trap)
    with stack:
        for addr in args.addrs:
            print(f'Creating an ssh server at: {addr}')
            stack.enter_context(tmp_ssh_server(host=addr, command=args.command))

        while _run:
            time.sleep(1)

        print('Closing down!')

    print('End')


if __name__ == '__main__':
    main()
