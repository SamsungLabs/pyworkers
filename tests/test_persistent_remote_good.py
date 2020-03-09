from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.remote_server import spawn_server

import time


def test_fun2(x):
    return x**2


if __name__ == '__main__':
    server = spawn_server('127.0.0.1', 6006)
    try:
        if not server.is_alive():
            if server.has_error:
                raise server.error
            else:
                raise Exception('Unknown error')

        p = PersistentRemoteWorker(test_fun2, host='127.0.0.1', name='P1')
        for i in range(1,10):
            p.enqueue(i)
        
        p.wait()
        print(p.has_error)
        print(p.result)
        print(p.error)
        for result in p.results_iter():
            print(result)
    finally:
        if server.is_alive():
            print('Terminating server')
            server.terminate(timeout=1, force=True)
