from pyworkers.persistent import PersistentWorker, WorkerType

import itertools

def foo(x):
    return x**2

if __name__ == '__main__':
    workers = []
    for wtype in WorkerType:
        kwargs = { 'target': foo }
        if wtype is WorkerType.REMOTE:
            kwargs['host'] = ('127.0.0.1', 60006)

        worker = PersistentWorker.create(wtype, **kwargs)
        workers.append(worker)

    for worker, x in zip(itertools.cycle(workers), range(10)):
        worker.enqueue(x)

    results = []
    for worker in workers:
        worker.wait()
        print(worker.result, list(worker.results_iter()))
