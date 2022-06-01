from pyworkers.pool import Pool
from pyworkers.worker import WorkerType


def foo(x):
    return x**2


if __name__ == '__main__':
    p = Pool(foo, name='Simple Pool')
    with p:
        for wtype in WorkerType:
            kwargs = {} # note that the target function is now passed to the Pool object!
            if wtype is WorkerType.REMOTE:
                kwargs['host'] = ('127.0.0.1', 60006)

            p.add_worker(wtype, **kwargs)

        for w in p.workers:
            print(w.userid, w.is_alive()) # userid can be set by the user to any value in the worker's construct, by default it is an index of the worker within the Pool

        results = p.run(iter(range(10)))
        # workers are still alive as long as we are within the `with pool` block!
        # let restart them and run some more things
        p.restart_workers()
        results.extend(p.run(iter(range(10, 20))))

    for w in p.workers:
        # not workers should be dead
        print(w.userid, w.is_alive(), w.result)

    print(sorted(results))
