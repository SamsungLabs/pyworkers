from pyworkers.worker import Worker, WorkerType

def foo(x):
    return x**2

if __name__ == '__main__':
    workers = []
    for wtype, x in zip(WorkerType, range(1, 4)):
        kwargs = {
            'target': foo,
            'args': (x, )
        }
        if wtype is WorkerType.REMOTE:
            kwargs['host'] = ('127.0.0.1', 6006)

        worker = Worker.create(wtype, **kwargs)
        workers.append(worker)

    results = []
    for worker in workers:
        worker.wait()
        results.append(worker.result)

    print(results)
