from pyworkers.utils import test_loop
from pyworkers.thread import ThreadWorker

import time


if __name__ == '__main__':
    t = ThreadWorker(test_loop, name='T1')
    time.sleep(0.1)
    assert t.terminate()
    t.wait()
    print(t.has_error)
    print(t.result)
    print(t.error)
