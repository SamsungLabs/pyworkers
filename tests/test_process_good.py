from pyworkers.utils import test_fun
from pyworkers.process import ProcessWorker

import time


if __name__ == '__main__':
    p = ProcessWorker(test_fun, name='P1')
    p.wait()
    print(p.has_error)
    print(p.result)
    print(p.error)
