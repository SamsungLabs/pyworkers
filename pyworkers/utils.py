import sys
import time
import ctypes
import logging
import platform
from inspect import getfullargspec


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

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, log_kwargs = self.process(msg, kwargs)
            self.logger._log(level, BraceMessage(msg, args, kwargs), (), 
                    **log_kwargs)

    def process(self, msg, kwargs):
        return msg, {key: kwargs[key] 
                for key in getfullargspec(self.logger._log).args[1:] if key in kwargs}


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
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(_ctype_tid, ctypes.py_object(exception))
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
