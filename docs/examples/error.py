from pyworkers.process import ProcessWorker

def suicide(x):
    raise RuntimeError('How is one supposed to live on this miserable world where x = {}?'.format(x))

if __name__ == '__main__':
    pw = ProcessWorker(target=suicide, args=(2,))
    pw.wait()
    print(pw.result)
    print(pw.has_error)
    print(pw.error)
