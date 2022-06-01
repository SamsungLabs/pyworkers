# pyworkers

PyWorkers is a (relatively) small library wrapping and extending standard `threading` and `multiprocessing` libraries to provide things such as:
 - uniform API,
 - support for both graceful and forceful termination,
 - out-of-the-box support for running the target function multiple times,
 - lightweight support for RPC.

See the following sections for more details:
 - [Motivation and basic functionality](#motivation-and-basic-functionality)
 - [Requirements](#requirements)
 - [Installation](#installation)
 - [Generating documentation](#generating-documentation)
 - [Running tests](#running-tests)
 - [Usage examples](#usage-examples)
 - [Contributing](#contributing)
 - [Reporting issues](#reporting-issues)
 - [License](#license)

## Motivation and basic functionality
Both `threading` and `multiprocessing` are great standard python packages enabling developers to quickly create and manage parallel applications by running different parts of their code in separate threads and processes respectively.
This is done with convenient API which mostly remains consistent between them.
However, as the libraries are designed to provide relatively low-level functionality, in order to be suitable for as many use-cases as possible, some things are left unaddressed.
PyWorkers tries to provide higher-level abstraction specialized towards running expensive functions which may take significant time to finish (in terms of hours) and potentially become unresponsive during their execution.
In order to do that and maximize usage of available computational resources, the library includes functionality allowing its users to seamlessly interchange and mix threads, processes and remote processes (RPC) as different ways of parallelizing their workloads, as well as enhances managing them.
Unlike `multiprocessing.dummy`, which also tries to provide uniform API between threads and processes, PyWorkers extends it by supporting remote execution and improves by implementing the exact same set of methods between all three types of workers (e.g. `DummyProcess` from `multiprocessing` does not support `terminate`, threads and processes use different names for their identifiers - `ident` and `pid` respectively - etc.).

The following is a quick summary of all the enhancements the library provides.

### Uniform API
Each worker, regardless of its underlying running mechanism, implements the same set of methods and attributes: `wait`, `terminate`, `is_alive`, `host`, `pid`, `tid`, `id`, `close` (and others) while at the same time trying to make sure their behaviour is consistent with other implementations.

For more details please see the API documentation.

### Graceful termination
`terminate` is available as a way of abnormally finishing execution of a worker. Unlike its relative from `multiprocessing`, the function provided by this library will first try to gracefully finish execution of the target function by raising a `WorkerTerminateError` exception in the target worker, thus allowing it to release all allocated resources etc. The time a worker is given to finish can be controlled by `timeout` parameter - if the worker hasn't finished by the time it passes, the function will then switch to forceful termination using `os.kill` with `SIGTERM` signal on Linux or `TerminateProcess` on Windows.


### Out-of-the-box support for consumers (persistent workers)
In addition to running the target function once (like in the standard libraries), PyWorkers includes a set of specialized classes which make it easier to run the function multiple times, processing incoming arguments in a streaming manner.
This type of execution is primarily useful when processing data using the producer-consumer paradigm and is conveniently supported by the means of `Pool` class (similar to the one from `multiprocessing` package).
Unlike the standard package, PyWorkers exposes consumer classes directly, making them usable in contexts where `Pool` object is not desired.
Also, the pool supports different workers types mixed together and, in case when termination of workers is requested, by default tries to soft-terminate them first (following semantics of `terminate` as implemented in the library).


### Lightweight RPC
This is done in the most lightweight and simple way possible, while meeting the behavioural requirements (e.g., support for soft termination).
Please note that the goal of this package is not to compete with some mature, production-grade RPC solutions but rather to give developers an easy way to switch from local to remote execution while prototyping or experimenting with their code.
We use plain TCP connections and (de)serialize any workload with standard `pickle`, which we extend a little bit to allow users to customize serialization of their objects for remote execution (this extension is not-invasive, meaning that any code that is not aware of it will run as if the extension was not there).

Our RPC implementation requires our server (`pyworkers.remote_server`) to run on the target machine - if you want to spawn a server ad-hoc on the target host, rather than keeping it running as a service etc., you can use our convenient `pyworkers.remote_server.tmp_ssh_server` function that creates and destroys a server. The method is intended to be used via Python's context manager API.

> **Note:** As the name of the method suggests, you need to be able to ssh to the server. This method basically creates and interacts with an `ssh` process, taking input from the `stdin` of the parent process and capturing output of the `ssh` command into a buffer that can be read by the caller.

## Requirements
 - Python3.6

No 3rd-party packages are used to provide core functionality, only standard `threading`, `multiprocessing` (including `multiprocessing.connection`), `socket`, `os`, `signal`, `pickle` and others.

`gitpython` is optional and provides additional versioning information if using the package via developer-mode installation with pip (see below).\
`setproctitle` is optional and allows changing names of processes and threads created with `pyworkers`, changing of names can be disabled/enabled on a per-worker basis.

### Tested with:
 - Ubuntu 18.04
 - Windows 10
 - WSL running Ubuntu 18.04

## Installation
Clone this repo, then using your desired version of `pip` run:
```bash
pip install .
```
from the root folder of this repo. If not using any environment virtualization tools, run the above command using `sudo` or with an extra `--user` argument passed to `pip` in order to install it in your user's local directory.

The above command will install the package by copying it to the python's `site-packages` folder - therefore, the installed version will not be synchronized with the changes you make in the cloned repo. In other words, each time you make changes, you will have to re-run the `pip install` command to make those changes visible in the installed package.

Alternatively, the package can be installed in a *developer mode* using the `-e` switch:
```bash
pip install -e .
```

## Generating documentation
> **Note:** The documentation is still in the early phase.
> Although quite a lot of thing has already been documented, there are some which are still missing, formatting might be broken at some places, and putting it all together might also require some extra work.

To automatically generate API documentation for the library, make sure `sphinx` and `sphinx_rtd_theme` are installed first.
You can easily install them with `pip`:
```bash
pip install sphinx sphinx_rtd_theme
```

> **Note:** `sphinx_rtd_theme` is only required if you want to use the readthedocs theme (the default one for this project).
> You can change the theme used by modifying your `docs/source/conf.py` file.

After installing `sphinx`, simply go to `docs/` and execute `make html`.
Your newly generated documentation should be available at `docs/build/index.html`.

## Running tests
The project uses standard `unittest` Python package for testing, the tests are stored within `tests/` subdirectory and follow the `*_test.py` naming convention.
The easiest way to run them is to navigate towards the root directory of this repo and from there run:
```bash
python3 -m unittest discover -v -s "." -p "*_test.py"
```
Alternatively, one can use VS Code with the provided `settings.json` from `.vscode` folder which configures the tests to be runnable from within the tests tab inside a VS Code window (using an analogical command to the above).
Each test file should also be runnable by its own.

## Usage examples
> **Tip:** You can find these examples under `docs/examples`!

```python
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

    rw = RemoteWorker(target=foo, args=(3,), host=('127.0.0.1', 60006))
    rw.wait()

    print(tw.result, pw.result, rw.result)
```

Should print `1, 4, 9`.

Please note that there's no extra `start` method which needs to be called after a worker is created. Instead, the constructor automatically spawns a worker to make sure that the object is valid (e.g. pid) as soon as it is created.

> **Note:** The remote worker in this example assumes that there is a remote server running locally on the TCP port 60006. This server can be created programmatically by including the following code (creates a child process):
> ```python
> ...
> from pyworkers.remote_server import spawn_server
> 
> if __name__ == '__main__':
>     server = spawn_server(('127.0.0.1', 60006)) # returns ProcessWorker!
>     try:
>         ...
>     finally:
>         server.terminate()
> ```
> or run as a standalone process by executing the `pyworkers.remote_server` module:
> ```bash
> python3 -m pyworkers.remote_server -v --addr 127.0.0.1 --port 60006
> ```
> It is also possible to run the server programatically in the calling thread by using `RemoteServer` class from `pyworkers.remote_server` directly, instead of using it via `spawn_server` and `RemoteServerProcess`, but care should be taken when doing so as `accept` (which is called on the server socket) is tricky to interrupt.

The same can be achieved without necessity of hardcoding the workers' types as one could instead use values from the `WorkerType` enum together with a factory classmethod `Worker.create`.

```python
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
            kwargs['host'] = ('127.0.0.1', 60006)

        worker = Worker.create(wtype, **kwargs)
        workers.append(worker)

    results = []
    for worker in workers:
        worker.wait()
        results.append(worker.result)

    print(results)
```

The code above should print the same result as the previous one.

> **Note:** the `Worker` class is the base class for all workers types.
> The shared API is defined within it.

> **Note:** although in the example we only set 'host' argument for remote workers, this extra check is not required. Following the principle of having a uniform API, all workers accept this argument but local workers (processes and threads) will simply ignore it.

### Handling errors

If an error happens when running the target function, the `result` field of the worker which failed will be set to `None` and instead `error` will hold the exception object which caused the failure.
For example:

```python
from pyworkers.process import ProcessWorker

def suicide(x):
    raise RuntimeError('How is one supposed to live on this miserable world where x = {}?'.format(x))

if __name__ == '__main__':
    pw = ProcessWorker(target=suicide, args=(2,))
    pw.wait()
    print(pw.result)
    print(pw.has_error)
    print(pw.error)
```

The above code should print:
```
<Exception reported from the child process>
None
True
How is one supposed to live on this miserable world where x = 2?
```

> **Note:** Although in almost all cases if an error happened the `error` field of a worker should be not None, it is possible for the worker to die due to a reason which is not reported via a Python exception.
> In that case, `has_error` will be set to `True` but `error` can be `None` (example of such situation could be when a child process is killed with SIGKILL signal on Linux).
> Therefore, it is better to test `has_error` field instead of `error` if a guarantee about erroneous exit is desired.

### Persistent Workers
If it is desired to run the target function multiple times without the overhead of creating new workers every time, it is possible to use their persistent variations.

Persistent workers take the target function as an argument when they are created (like standard workers) but will wait for the incoming arguments until they are `close`d, returning results to the caller via a dedicated queue/pipe/socket and the standard `worker.result` (common for all workers) field will eventually hold the total number of times the worker has executed its function (which should be the same as the number of elements in the results queue).
A new call to the function, with new arguments, can be done by using the `.enqueue` method.
To maintain consistent behaviour with other workers, `.result` attribute is only set after the worker has finished the entirety of its work, which for persistent workers means that all enqueued arguments have been processed and the worker has been closed.
Unlike the final `.result`, results of individual calls can be obtained as soon as they are available (i.e., the worker does not have to be closed) using either `.get_next_result` (which can be blocking or not) or a higher-level `.get_results_iter` - see documentation for more information!

Just like with ordinary workers, it is possible to create persistent workers using their classes directly (`PersistentThreadWorker`, `PersistentProcessWorker` and `PersistentRemoteWorker`) or by using the factory classmethod `PersistentWorker.create`.
For example:

```python
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
```

Expected output:
```
4 [0, 9, 36, 81]
3 [1, 16, 49]
3 [4, 25, 64]
```

> **Note:** persistent workers can be restarted by the user if it is desired to, e.g., free some resources that might accumulate over long periods of time (a good example might be some GPU memory that will not get deallocated as long as a process is running).
> Restarting is done by completely removing any underlying entities (such as processes) and creating new one in their place, encapsulated within the same Python object and using the same arguments as those used by the original worker.

### Pool
If multiple workers are used to execute the same function, it is possible to use a single-producer multiple-consumer pattern that we implement with our `Pool` class.
The `Pool` class is very similar to its equivalent from the standard `multiprocessing` package, so we hope that the core idea is rather self-explanatory.
Unlike the standard process pool, our class allows mixing different worker types, supports callbacks for high customizability, and gives the user more control over the lifetime of workers (e.g., when processing of an input sequence has finished, the workers will not be killed and the user is free to start processing a new input sequence).

The basic usage is showed in the example below:

```python
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

```

Expected output (similar to):

```
0 True
1 True
2 True
0 False 4
1 False 4
2 False 2
[0, 1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121, 144, 169, 196, 225, 256, 289, 324, 361]
```

> **Note:** At the moment, the `Pool` object only supports persistent workers but they can be restarted manually by user (e.g., as in the example above).

> **Note:** More advanced usage of the `Pool` object actually allows to customize what function different workers will execute.
> However, a care should be taken in those cases as different workers might expect different types/number of arguments - to support cases like that,
> instead of giving a simple iterator to `Pool.run`, we can pass a callable that, when given a particular worker, should return suitable arguments for it.

> **Note:** As can be seen in this example, restarting workers resets their `.result` counter, therefore the last values in each line printed by `print(w.userid, w.is_alive(), w.result)` should add up only to 10. Without `p.restart_workers()` they should sum to 20.

### More examples

For more examples, consider looking at the tests defined in the `tests/` subfolder!

## Contributing

All contributions are welcome, please open a pull request with your changes!

If a substantial change is accepted and merged into the codebase the author might be asked to own contributed pieces of code and become responsible for reviewing/maintaining those parts.
Lack of commitment to fulfil this obligation might result in reverting any changes, arbitrary changes of ownership, or any other actions deemed necessary to allow for healthy development of the package.

When making your changes, please follow the coding style used throughout the project (PEP-8).

## Reporting issues

Please open an issue on GitHub and provide minimal failing example, together with information about the specific version of the package (ideally git commit), Python version and OS used.

## License

The package is released under Apache License 2.0.
See LICENSE file for more information.
