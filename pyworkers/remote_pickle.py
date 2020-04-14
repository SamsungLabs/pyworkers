''' This module implements an extension to the pickling mechanism which allows a user to specialize serialization and deserialization
    of objects for remote transfer.
    By default, `__getstate__` and `__setstate__` are used to enable customisation of pickling process, however in some cases
    it might be useful to distinguish between different pickling contexts. For example, pickling of a file object will be handled
    differently when an object is pickled for IPC (i.e. it can be done with Unix's `dup`) and differently when transmitted over
    network (in which case a completely custom mechanism must be implemented). Unfortunately, the default pickling mechanism does not
    allow specialization of `__getstate__` and therefore distinguishing between different pickling cases requires some extra work.
    This module tries to ease the burden of detecting serialization requests when requested for network transfer, while at the same
    time being compatible with the standard pickle module, as much as possible.

    Specifically, the module provides support for an extended `__getstate__` signature of the objects being pickled, by checking for
    an extra argument `remote` and setting it to `True` when an object is being pickled with `remote_pickler`. Internally, that is
    done by a means of a metaclass `SupportRemoteGetStateMeta` and dispatch tables mechanism within the standard `Pickler` class.
    The metaclass is responsible for registering all classes which contain a `__getstate__` function with the extended signature (i.e. with `remote`
    argument). The registered classes are later used to populate a dispatch table in the `RemotePickler` instance to reduce them with a reduce
    function which follows the default reduce behaviour but calls `__getstate__(remote=True)` instead of just `__getstate__()`.
    Additionally, when unpickling, it is possible to dynamically modify state of the 
    For more information about internal mechanism, its limitations etc., please look at documentation of `

    To make a class compatible with the extended `__getstate__` 
'''

from .utils import python_is_exactly

import io
import copyreg
import inspect


class SupportRemoteGetStateMeta(type):
    supported_classes = []
    def __init__(cls, name, bases, dict):
        super().__init__(name, bases, dict)
        allow_remote = True
        first_not_remote = None
        has_remote = False
        for base in cls.__mro__[:-1]:
            d = base.__dict__
            if d.get('__reduce_ex__') or d.get('__reduce__'):
                has_remote = False
                break
            if d.get('__getstate__'):
                signature = inspect.signature(d.get('__getstate__'))
                param_names = [param.name for param in signature.parameters.values()]
                param_kinds = [param.kind for param in signature.parameters.values()]
    
                if 'remote' in param_names:
                    if not allow_remote:
                        msg = 'A base class {!r} of class {!r}'.format(first_not_remote.__name__, cls.__name__) if first_not_remote is not cls else 'A class {!r}'.format(cls.__name__)
                        msg += ' does not support "remote" argument to __getstate__ but one of its base classes ({!r}) does. This inconsistency can be potentially a source of problems.'.format(base.__name__)
                        raise Warning(msg)
                    has_remote = True
                elif inspect.Parameter.VAR_KEYWORD in param_kinds:
                    continue
                else:
                    allow_remote = False
                    first_not_remote = base

        if has_remote:
            assert cls not in cls.supported_classes
            cls.supported_classes.append(cls)


class SupportRemoteGetState(metaclass=SupportRemoteGetStateMeta):
    ''' 
    '''
    pass


#if python_is_exactly(3, 6):
#    from ._remote_pickle.remote_pickler_3_6 import RemotePickler36 as RemotePickler
#else:
#    raise RuntimeError('Unsupported python version')

from ._remote_pickle.remote_pickler_3_6 import RemotePickler36 as RemotePickler


import pickle


def remote_dump(obj, file, protocol=None, remote=True, **kwargs):
    p = RemotePickler(file, protocol, remote=remote, **kwargs)
    p.dump(obj)
    import pickle
    pickle.dump()


def remote_dumps(obj, protocol=None, remote=True, **kwargs):
    buff = io.BytesIO()
    p = RemotePickler(buff, protocol, remote=remote, **kwargs)
    p.dump(obj)
    return buff.getvalue()


def remote_load(file, extra_kwargs=None, **kwargs):
    from ._remote_pickle.state import RemoteState
    extra_kwargs = extra_kwargs or {}
    with RemoteState.context(extra_kwargs):
        return pickle.load(file, **kwargs)

def remote_loads(buff, extra_kwargs=None, **kwargs):
    from ._remote_pickle.state import RemoteState
    extra_kwargs = extra_kwargs or {}
    with RemoteState.context(extra_kwargs):
        return pickle.loads(buff, **kwargs)


# aliases for intercompatibility with standard pickle module
dump = remote_dump
dumps = remote_dumps
load = remote_load
loads = remote_loads
Pickler = RemotePickler
