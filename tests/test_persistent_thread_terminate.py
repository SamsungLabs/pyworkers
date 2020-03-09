from pyworkers.utils import test_fun2
from pyworkers.persistent_thread import PersistentThreadWorker

import time


if __name__ == '__main__':
    p = PersistentThreadWorker(test_fun2, name='P1')
    for i in range(1,10):
        p.enqueue(i)

    time.sleep(0.1)
    assert p.terminate()
    
    print(p.has_error)
    print(p.result)
    print(p.error)
    for result in p.results_iter():
        print(result)
