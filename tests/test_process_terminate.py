from pyworkers.utils import test_loop
from pyworkers.process import ProcessWorker

import time


if __name__ == '__main__':
    p = ProcessWorker(test_loop, name='P1')
    time.sleep(0.1)
    assert p.terminate()
    p.wait()
    print(p.has_error)
    print(p.result)
    print(p.error)
    raise p.error
