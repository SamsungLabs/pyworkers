import pickle
import copyreg
from collections import OrderedDict

from .state import RemoteState


class RemotePickler36(pickle.Pickler):
    @staticmethod
    def subject_to_custom_reduce(obj):
        from ..remote_pickle import SupportRemoteGetState
        return type(obj) in SupportRemoteGetState.supported_classes

    @staticmethod
    def remote_reduce(obj):
        assert getattr(type(obj), '__reduce_ex__') is object.__reduce_ex__, obj.__reduce_ex__
        assert getattr(type(obj), '__reduce__') is object.__reduce__, obj.__reduce__

        # Follows C implementation from https://github.com/python/cpython/blob/1b55b65638254aa78b005fbf0b71fb02499f1852/Objects/typeobject.c#L4489
        # replace __getstate__() with __getstate__(remote=True)

        # _PyObject_GetNewArguments from https://github.com/python/cpython/blob/1b55b65638254aa78b005fbf0b71fb02499f1852/Objects/typeobject.c#L4348
        args, kwargs = None, None
        if hasattr(obj, '__getnewargs_ex__'):
            args, kwargs = obj.__getnewargs_ex__()
            if not isinstance(args, tuple):
                raise TypeError( 'first item of the tuple returned by __getnewargs_ex__ must be a tuple, not {!r}'.format(type(args).__name__))
            if not isinstance(kwargs, dict):
                raise TypeError( 'second item of the tuple returned by __getnewargs_ex__ must be a dict, not {!r}'.format(type(kwargs).__name__))
        elif hasattr(obj, '__getnewargs__'):
            args = obj.__getnewargs__()
            if not isinstance(args, tuple):
                raise TypeError('__getnewargs__ should return a tuple, not {!r}'.format(type(args).__name__))

        if not kwargs:
            newobj = copyreg.__newobj__
            args = args or tuple()
            newargs = (type(obj), *args)
        elif args:
            newobj = copyreg.__newobj_ex__
            newargs = (type(obj), args, kwargs)
        else:
            raise RuntimeError('Internal bad call')

        # _PyObject_GetState from https://github.com/python/cpython/blob/1b55b65638254aa78b005fbf0b71fb02499f1852/Objects/typeobject.c#L4207
        state = obj.__getstate__(remote=True)

        # _PyObject_GetItemsIter from https://github.com/python/cpython/blob/1b55b65638254aa78b005fbf0b71fb02499f1852/Objects/typeobject.c#L4444
        listitems = None if not isinstance(obj, list) else obj.__iter__()
        dictitems = None if not isinstance(obj, dict) else obj.items().__iter__()

        children_names = []
        if isinstance(state, dict):
            state = OrderedDict(state)
            for key, value in state.items():
                if RemotePickler36.subject_to_custom_reduce(value):
                    children_names.append(key)

        newargs = (newobj, newargs, children_names)
        newobj = RemoteState.recreate_obj_and_patch_setstate

        return (newobj, newargs, state, listitems, dictitems)

    def __init__(self, *args, **kwargs):
        from ..remote_pickle import SupportRemoteGetState
        super().__init__(*args, **kwargs)
        self.dispatch_table = {}
        for cls in SupportRemoteGetState.supported_classes:
            self.dispatch_table[cls] = RemotePickler36.remote_reduce
