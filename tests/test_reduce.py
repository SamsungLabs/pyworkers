import io
import pickle
import copyreg

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[1]))

import pickle
import workers.remote_pickle as remote_pickle


class Foo(remote_pickler.SupportRemoteGetState):
    def __getstate__(self, remote=False):
        return 0 if not remote else 1
    def __setstate__(self, state):
        print(type(self), state)

class Bar(Foo):
    pass

class Dip():
    def __getstate__(self):
        return 2
    def __setstate__(self, state):
        print(type(self), state)

class Pap(Foo):
    def __getstate__(self, **kwargs):
        return super().__getstate__(**kwargs) * 10

class Tup(remote_pickler.SupportRemoteGetState):
    def __getstate__(self):
        return 4
    def __setstate__(self, state):
        print(type(self), state)

class Wep(Tup):
    def __getstate__(self, **kwargs):
        return super().__getstate__(**kwargs) * 100

class WithPatchedState(remote_pickler.SupportRemoteGetState):
    def __getstate__(self, remote=False):
        return { 'test': 1, 'test2': 10 } if remote else 0

    def __setstate__(self, state):
        print(type(self), state)

class NestedPatched1(remote_pickler.SupportRemoteGetState):
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
        print(type(self), self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

class NestedPatched2(remote_pickler.SupportRemoteGetState):
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
        print(type(self), self.__dict__)


orig = (Bar(), Foo(), Dip(), Pap(), Tup(), Wep())
print('Remote pickle')
data = remote_pickler.dumps(orig)
new = remote_pickler.loads(data)

print('Standard pickle')
data2 = pickle.dumps(orig)
new2 = pickle.loads(data2)

o = WithPatchedState()
print('Patched remote')
data3 = remote_pickler.dumps(o)
new3 = remote_pickler.loads(data3, { 'test': 2 })

print('Patched standard')
data4 = pickle.dumps(o)
new4 = pickle.loads(data4)

o2 = NestedPatched2()
print('Nested remote')
data5 = remote_pickler.dumps(o2)
new5 = remote_pickler.loads(data5)

print('Nested patched remote')
data6 = remote_pickler.dumps(o2)
new6 = remote_pickler.loads(data6, { 'special': 8, 'nested': { 'special': 16 } })

print('Nested standard')
data7 = pickle.dumps(o2)
new7 = pickle.loads(data7)

print('Nested patched remote 2')
data8 = remote_pickler.dumps(o2)
new8 = remote_pickler.loads(data8, { 'nested': { 'special': 16 } })

print('Nested patched remote 3')
data9 = remote_pickler.dumps(o2)
new9 = remote_pickler.loads(data9, { 'nested': -1 })

print('Nested patched remote 4')
data10 = remote_pickler.dumps(o2)
new10 = remote_pickler.loads(data10, { 'special': 8 })
