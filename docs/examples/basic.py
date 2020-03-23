from pyworkers.thread import ThreadWorker
from pyworkers.process import ProcessWorker
from pyworkers.remote import RemoteWorker

def foo(x):
    return x**2

if __name__ == '__main__':
    tw = ThreadWorker(target=foo, args=(1,))
    tw.wait()

    pw = ProcessWorker(target=foo, args=(2,))
    pw.wait()

    rw = RemoteWorker(target=foo, args=(3,), host=('127.0.0.1', 6006))
    rw.wait()

    print(tw.result, pw.result, rw.result)
