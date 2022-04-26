import sys
import time
import queue
import ctypes
import logging
import platform
import threading
import multiprocessing as mp
from inspect import getfullargspec


STATUS = int((logging.INFO + logging.WARNING) / 2)
DETAILS = int((logging.DEBUG + logging.INFO) / 2)
ABUSIVE = int((logging.NOTSET + logging.DEBUG) / 2)
logging.addLevelName(STATUS, 'STATUS')
logging.addLevelName(DETAILS, 'DETAILS')
logging.addLevelName(ABUSIVE, 'ABUSIVE')


class BraceMessage(object):
    def __init__(self, fmt, args, kwargs):
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.fmt).format(*self.args, **self.kwargs)

    def __repr__(self):
        return super().__repr__() + self.__str__()


class BraceStyleAdapter(logging.LoggerAdapter):
    def __init__(self, logger):
        self.logger = logger

    def status(self, msg, *args, **kwargs):
        return self.log(STATUS, msg, *args, **kwargs)

    def details(self, msg, *args, **kwargs):
        return self.log(DETAILS, msg, *args, **kwargs)

    def abusive(self, msg, *args, **kwargs):
        return self.log(ABUSIVE, msg, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, log_kwargs = self.process(msg, kwargs)
            self.logger._log(level, BraceMessage(msg, args, kwargs), (), 
                    **log_kwargs)

    def process(self, msg, kwargs):
        return msg, {key: kwargs[key] 
                for key in getfullargspec(self.logger._log).args[1:] if key in kwargs}


def get_logger(name=None):
    return BraceStyleAdapter(logging.getLogger(name))


class classproperty(property):
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        if fget is not None and not isinstance(fget, classmethod):
            raise ValueError('fget should be a classmethod')
        if fset is not None and not isinstance(fset, classmethod):
            raise ValueError('fset should be a classmethod')
        if fdel is not None and not isinstance(fdel, classmethod):
            raise ValueError('fdel should be a classmethod')
        super().__init__(fget, fset, fdel, doc)
        if doc is None and fget is not None:
            self.__doc__ = fget.__func__.__doc__

    def __get__(self, inst, cls=None):
        if self.fget is None:
            raise AttributeError("unreadable attribute")

        return self.fget.__get__(inst, cls)(inst=inst) # pylint: disable=no-member

    def __set__(self, inst, val):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        if isinstance(inst, type):
            cls = inst
            inst = None
        else:
            cls = type(inst)
        return self.fset.__get__(inst, cls)(val, inst=inst) # pylint: disable=no-member

    def __delete__(self, inst):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        if isinstance(inst, type):
            cls = inst
            inst = None
        else:
            cls = type(inst)
        return self.fdel.__get__(inst, cls)(inst=inst) # pylint: disable=no-member


class staticproperty(property):
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        super().__init__(fget, fset, fdel, doc)
        if doc is None and fget is not None:
            try:
                self.__doc__ = fget.__func__.__doc__
            except AttributeError:
                self.__doc__ = fget.__doc__

    def __get__(self, inst, cls=None):
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget()

    def __set__(self, inst, val):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        return self.fset(val)

    def __delete__(self, inst):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        return self.fdel()


class SupportClassPropertiesMeta(type):
    def __setattr__(self, key, value):
        obj = self.__dict__.get(key)
        if obj and type(obj) is classproperty:
            return obj.__set__(self, value)

        return super(SupportClassPropertiesMeta, self).__setattr__(key, value)

    def __delattr__(self, key):
        obj = self.__dict__.get(key)
        if obj and type(obj) is classproperty:
            return obj.__delete__(self)

        return super(SupportClassPropertiesMeta, self).__delattr__(key)


class PipeEndpoint():
    def __init__(self, pipe):
        self._pipe = pipe

    @property
    def stdpipe(self):
        return self._pipe

    def fileno(self):
        return self._pipe.fileno()

    def send(self, obj):
        return self._pipe.send(obj)

    def recv(self):
        return self._pipe.recv()

    def poll(self, timeout=0):
        if time == 0:
            return self._pipe.poll()
        else:
            return self._pipe.poll(timeout)

    def close(self):
        return self._pipe.close()

    def put(self, obj):
        return self._pipe.send(obj)

    def get(self, block=True, timeout=None):
        if not block:
            try:
                if not self._pipe.poll():
                    raise queue.Empty
            except (BrokenPipeError, OSError):
                raise queue.Empty

        try:
            return self._pipe.recv()
        except (EOFError, BrokenPipeError):
            raise queue.Empty

    def get_nowait(self):
        return self.get(block=False)


class Pipe():
    def __init__(self):
        self._endpoints = tuple(PipeEndpoint(pipe) for pipe in mp.Pipe())

    @property
    def parent_end(self):
        return self._endpoints[0]

    @property
    def child_end(self):
        return self._endpoints[1]


class Queue(queue.Queue):
    def close(self):
        pass


class LocalPipe():
    def __init__(self):
        self._q = Queue()

    @property
    def parent_end(self):
        return self._q

    @property
    def child_end(self):
        return self._q


def get_hostname():
    return platform.node()


def python_is_at_least(major, minor):
    return sys.version_info[0] > major or (sys.version_info[0] == major and sys.version_info[1] >= minor)


def python_is_exactly(major, minor):
    return sys.version_info[0] == major and sys.version_info[1] == minor


def is_windows():
    return sys.platform.lower().startswith("win")


def foreign_raise(tid, exception):
    _ctype_tid = ctypes.c_ulong(tid) if python_is_at_least(3, 7) else ctypes.c_long(tid)
    ctypes.pythonapi.PyThreadState_SetAsyncExc(_ctype_tid, ctypes.py_object())
    _ctype_ex_obj = ctypes.py_object(exception) if exception is not None else ctypes.py_object()
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(_ctype_tid, _ctype_ex_obj)
    # ref: http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc
    if ret == 0:
        raise ValueError("Invalid Thread ID")
    elif ret > 1:
        # Huh? Why would we notify more than one threads?
        # Because we punch a hole into C level interpreter.
        # So it is better to clean up the mess.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(_ctype_tid, ctypes.py_object())
        raise SystemError("PyThreadState_SetAsyncExc failed")


def active_sleep(timeout, interval=0.01):
    total_time = 0
    while total_time < timeout:
        to_sleep = min(interval, timeout - total_time)
        time.sleep(to_sleep)
        total_time += to_sleep



def active_wait(obj, timeout, interval=0.01):
    if timeout < 0:
        raise ValueError('Negative timeout')

    if timeout is None:
        obj.join()
        return True
    
    total_time = 0
    while obj.is_alive() and total_time < timeout:
        to_sleep = min(interval, timeout - total_time)
        time.sleep(to_sleep)
        total_time += to_sleep

    return obj.is_alive()


class LazyModule():
    def __init__(self, module):
        self.module = module

    def __getattr__(self, name):
        return getattr(self.module, name)


def add_module_properties(module_name, properties):
    module = sys.modules[module_name]
    replace = False
    if isinstance(module, LazyModule):
        lazy_type = type(module)
    else:
        lazy_type = type('LazyModule({})'.format(module_name), (LazyModule,), {})
        replace = True

    for name, prop in properties.items():
        setattr(lazy_type, name, prop)

    if replace:
        sys.modules[module_name] = lazy_type(module)


if python_is_at_least(3, 8):
    def gettid(thread=None):
        if thread is not None:
            return thread.native_id
        return threading.get_native_id()
else:
    def gettid(thread=None):
        if thread is not None:
            return thread.ident
        return threading.get_ident()


def typename(obj):
    n = type(obj).__qualname__
    p = type(obj).__module__
    if p:
        return f'{p}.{n}'
    return n


def setproctitle(title, worker=None):
    try:
        import setproctitle as _impl
        worker_info = f' {typename(worker)}' if worker is not None else ''
        _full_title = f'{sys.executable}{worker_info} {title}'
        _impl.setproctitle(_full_title)
    except:
        return


def setthreadtitle(title, worker=None):
    try:
        import setproctitle as _impl
        worker_info = f' {typename(worker)}' if worker is not None else ''
        _full_title = f'{sys.executable}{worker_info} {title}'
        _impl.setthreadtitle(_full_title)
    except:
        return
