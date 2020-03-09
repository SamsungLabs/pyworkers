from pyworkers.utils import test_fun2
from pyworkers.persistent_remote import PersistentRemoteWorker
from pyworkers.remote_server import spawn_server

import time


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
        
        time.sleep(0.5)
    finally:
        if server.is_alive():
            print('Terminating server')
            server.terminate(timeout=1, force=True)

    assert not p.is_alive()
    print(p.has_error)
    print(p.result)
    print(p.error)
    for result in p.results_iter():
        print(result)
