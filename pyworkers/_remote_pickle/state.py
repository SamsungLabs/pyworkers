import threading
from collections import namedtuple


class fake_threading_local():
    pass

class RemoteState(dict):
    _active_contexts = threading.local()
    #_active_contexts = fake_threading_local
    _patches_t = namedtuple('PatchesInfo', ['parent_i', 'name', 'patches'])

    class context(dict):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            assert not hasattr(RemoteState._active_contexts, 'ctxs')
            RemoteState._active_contexts.stack = []
            RemoteState._active_contexts.iter = -1

        def __enter__(self):
            if self:
                RemoteState._active_contexts.stack.append(RemoteState._patches_t(-1, None, self))
                RemoteState._active_contexts.iter = 0

        def __exit__(self, *exc):
            if exc[0] is None:
                assert RemoteState._active_contexts.iter == -1, RemoteState._active_contexts.iter
                assert not RemoteState._active_contexts.stack, RemoteState._active_contexts.stack
                del RemoteState._active_contexts.stack
                del RemoteState._active_contexts.iter

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def patches_iter(cls):
        return cls._active_contexts.iter

    @classmethod
    def increment_patches_iter(cls):
        cls._active_contexts.iter += 1

    @classmethod
    def decrement_patches_iter(cls):
        cls._active_contexts.iter -= 1

    @classmethod
    def set_patches_iter(cls, it):
        cls._active_contexts.iter = it

    @classmethod
    def get_patches_info(cls, idx):
        return cls._active_contexts.stack[idx]

    @classmethod
    def get_current_patches_info(cls):
        i = cls.patches_iter()
        if i < 0:
            return RemoteState._patches_t(-1, None, {})
        return cls.get_patches_info(cls.patches_iter())

    @classmethod
    def get_current_patches_parent_info(cls):
        parent_i = cls.get_current_patches_info().parent_i
        if parent_i < 0:
            return RemoteState._patches_t(-1, None, {})
        return cls.get_patches_info(parent_i)

    @classmethod
    def current_patches(cls):
        ''' Patches for the currently processed object's state.
        '''
        return cls.get_current_patches_info().patches

    @classmethod
    def current_child_name(cls):
        ''' Name of the currently processed object within its parent.
            Return None for the top-level object.
        '''
        return cls.get_current_patches_info().name

    @classmethod
    def parent_patches(cls):
        ''' Patches for the currently processed object's parent's state.
        '''
        return cls.get_current_patches_parent_info().patches

    @classmethod
    def close_current_ctx(cls):
        i = cls.patches_iter()
        if i < 0:
            return
        del cls._active_contexts.stack[cls.patches_iter()]
        cls.decrement_patches_iter()

    @classmethod
    def child_restored(cls, obj):
        assert cls.patches_iter() == len(cls._active_contexts.stack) - 1
        patches = cls.current_patches()
        if patches is not None:
            obj_name = cls.current_child_name()
            parent_patches = cls.parent_patches()
            assert bool(parent_patches) == bool(obj_name)
            if parent_patches:
                parent_patches[obj_name] = obj

        cls.close_current_ctx()

    @classmethod
    def break_patches(cls, names):
        it = cls.patches_iter()
        patches = cls.current_patches()
        sub_patches = []
        for name in names:
            dummy = True
            if patches is not None and name in patches:
                sub = patches[name]
                if isinstance(sub, dict):
                    sub_patches.append(RemoteState._patches_t(it + len(sub_patches), name, sub))
                    dummy = False

            if dummy:
                sub_patches.append(RemoteState._patches_t(-1, None, {}))
        
        if sub_patches:
            cls._active_contexts.stack[it+1:it+1] = sub_patches
            cls.increment_patches_iter()

    @staticmethod
    def recreate_obj_and_patch_setstate(newobj, newargs, children_names):
        ret = newobj(*newargs)
        orig_getstate = ret.__setstate__.__func__
        def patched_setstate(obj, state):
            if isinstance(state, dict):
                patched_state = state.copy()
                patched_state.update(RemoteState.current_patches())
            elif RemoteState.current_patches():
                raise TypeError('State should be dict in order to be patched, not {!r}, while patching remote state of an object with type {!r} with patching context: {}'.format(type(state).__name__, type(ret).__name__, RemoteState._active_contexts.ctxs[-1]))
            else:
                patched_state = state
            del obj.__setstate__
            assert obj.__setstate__.__func__ is orig_getstate
            orig_getstate(obj, patched_state)
            RemoteState.child_restored(obj)

        ret.__setstate__ = patched_setstate.__get__(ret, type(ret)) # pylint: disable=assignment-from-no-return,no-value-for-parameter
        RemoteState.break_patches(children_names)
        return ret
