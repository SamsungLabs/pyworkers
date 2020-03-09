from pyworkers.utils import test_fun
from pyworkers.thread import ThreadWorker

import time


if __name__ == '__main__':
    t = ThreadWorker(test_fun, name='T1')
    t.wait()
    print(t.has_error)
    print(t.result)
    print(t.error)
