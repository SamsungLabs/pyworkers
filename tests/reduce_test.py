import unittest

import pickle
import pyworkers.remote_pickle as remote_pickle


class Foo(remote_pickle.SupportRemoteGetState):
    def __getstate__(self, remote=False):
        return 0 if not remote else 1
    def __setstate__(self, state):
        self.val = state

class Bar(Foo):
    pass

class Dip():
    def __getstate__(self):
        return 2
    def __setstate__(self, state):
        self.val = state

class Pap(Foo):
    def __getstate__(self, **kwargs):
        return super().__getstate__(**kwargs) * 10

class Tup(remote_pickle.SupportRemoteGetState):
    def __getstate__(self):
        return 4
    def __setstate__(self, state):
        self.val = state

class Wep(Tup):
    def __getstate__(self, **kwargs):
        return super().__getstate__(**kwargs) * 100

class WithPatchedState(remote_pickle.SupportRemoteGetState):
    def __getstate__(self, remote=False):
        return { 'test': 1, 'test2': 10 } if remote else 0

    def __setstate__(self, state):
        self.val = state

class NestedPatched1(remote_pickle.SupportRemoteGetState):
    def __init__(self):
        self.special = 0
        self.normal = 1

    def __getstate__(self, remote=False):
        if not remote:
            return self.__dict__.copy()

        d = self.__dict__.copy()
        d['special'] = None
        return d

    def __setstate__(self, state):
        if state.get('special') is None:
            state['special'] = 2

        self.__dict__.update(state)

    def __repr__(self):
        return str(self.__dict__)

class NestedPatched2(remote_pickle.SupportRemoteGetState):
    def __init__(self):
        self.special = 0
        self.normal = 3
        self.nested = NestedPatched1()

    def __getstate__(self, remote=False):
        if not remote:
            return self.__dict__.copy()

        d = self.__dict__.copy()
        d['special'] = None
        return d

    def __setstate__(self, state):
        if state.get('special') is None:
            state['special'] = 4

        self.__dict__.update(state)


class ImplicitRemote():
    def __init__(self):
        self.from_remote = False

    def __getstate__(self, remote=False):
        ret = self.__dict__.copy()
        ret['from_remote'] = remote
        return ret

    def __setstate__(self, state):
        self.__dict__.update(state)


class ShadowedRemote(ImplicitRemote):
    def __init__(self):
        super().__init__()
        self.foo = 10

    def __getstate__(self):
        state = super().__getstate__()
        state['foo'] = 0
        return state


class RemotePickleTest(unittest.TestCase):
    def test_simple_remote(self):
        orig = (Foo(), Bar(), Dip(), Pap(), Tup(), Wep())
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data)
        self.assertEqual(tuple(obj.val for obj in new), (1, 1, 2, 10, 4, 400))

    def test_simple_standard(self):
        orig = (Foo(), Bar(), Dip(), Pap(), Tup(), Wep())
        data = pickle.dumps(orig)
        new = pickle.loads(data)
        self.assertEqual(tuple(obj.val for obj in new), (0, 0, 2, 0, 4, 400))

    def test_patched_state_remote_not_patched(self):
        orig = WithPatchedState()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data)
        self.assertIsInstance(new.val, dict)
        self.assertEqual(new.val['test'], 1)
        self.assertEqual(new.val['test2'], 10)

    def test_patched_state_remote_patched(self):
        orig = WithPatchedState()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data, { 'test': 2 })
        self.assertIsInstance(new.val, dict)
        self.assertEqual(new.val['test'], 2)
        self.assertEqual(new.val['test2'], 10)

    def test_patched_state_standard(self):
        orig = WithPatchedState()
        data = pickle.dumps(orig)
        new = pickle.loads(data)
        self.assertEqual(new.val, 0)

    def test_nested_patched_remote_not_patched(self):
        orig = NestedPatched2()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data)
        self.assertEqual(new.nested.normal, 1)
        self.assertEqual(new.nested.special, 2)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 4)

    def test_nested_patched_remote_patched_both(self):
        orig = NestedPatched2()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data, { 'special': 8, 'nested': { 'special': 16 } })
        self.assertEqual(new.nested.normal, 1)
        self.assertEqual(new.nested.special, 16)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 8)

    def test_nested_patched_remote_patched_nested(self):
        orig = NestedPatched2()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data, { 'nested': { 'special': 16 } })
        self.assertEqual(new.nested.normal, 1)
        self.assertEqual(new.nested.special, 16)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 4)

    def test_nested_patched_remote_patched_top(self):
        orig = NestedPatched2()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data, { 'special': 8 })
        self.assertEqual(new.nested.normal, 1)
        self.assertEqual(new.nested.special, 2)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 8)

    def test_nested_patched_remote_override_nested(self):
        orig = NestedPatched2()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data, { 'nested': -1 })
        self.assertEqual(new.nested, -1)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 4)

    def test_nested_patched_standard(self):
        orig = NestedPatched2()
        data = pickle.dumps(orig)
        new = pickle.loads(data)
        self.assertEqual(new.nested.normal, 1)
        self.assertEqual(new.nested.special, 0)
        self.assertEqual(new.normal, 3)
        self.assertEqual(new.special, 0)

    def test_implicit(self):
        orig = ImplicitRemote()
        data = remote_pickle.dumps(orig)
        new = remote_pickle.loads(data)
        self.assertFalse(orig.from_remote)
        self.assertTrue(new.from_remote)

    def test_implicit_standard(self):
        orig = ImplicitRemote()
        data = pickle.dumps(orig)
        new = pickle.loads(data)
        self.assertFalse(orig.from_remote)
        self.assertFalse(new.from_remote)

    def test_shadowed(self):
        orig = ShadowedRemote()
        with self.assertRaisesRegex(Warning, r'''A class 'ShadowedRemote' does not support "remote" argument to __getstate__ but one of its base classes \('.*'\) does. This inconsistency can be potentially a source of problems.'''):
            remote_pickle.dumps(orig)

    def test_shadowed_standard(self):
        orig = ShadowedRemote()
        pickle.dumps(orig)

    def test_shadowed_fake_standard(self):
        orig = ShadowedRemote()
        remote_pickle.dumps(orig, remote=False)

if __name__ == '__main__':
    unittest.main()
