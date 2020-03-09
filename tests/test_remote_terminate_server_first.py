from pyworkers.utils import test_loop
from pyworkers.remote import RemoteWorker
from pyworkers.remote_server import spawn_server

import time


if __name__ == '__main__':
    server = spawn_server('127.0.0.1', 6006)
    try:
        if not server.is_alive():
            raise server.error

        p = RemoteWorker(test_loop, host='127.0.0.1', name='P1')
        time.sleep(0.5)
    finally:
        if server.is_alive():
            print('Terminating server')
            server.terminate(timeout=1, force=True)

    assert not p.is_alive()
    print(p.has_error)
    print(p.result)
    print(p.error)