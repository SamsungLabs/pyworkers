from pyworkers.persistent_process import PersistentProcessWorker

import time

def test_fun2(x):
    return x**2

if __name__ == '__main__':
    p = PersistentProcessWorker(test_fun2, name='P1')
    for i in range(1,10):
        p.enqueue(i)

    p.wait()
    print(p.has_error)
    print(p.result)
    print(p.error)
    for result in p.results_iter():
        print(result)
