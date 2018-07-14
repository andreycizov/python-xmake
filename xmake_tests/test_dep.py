import string
import unittest

from dataclasses import dataclass

from xmake.dep import Deps, NotCreated, KeyedDeps


@dataclass
class Counter:
    x: int = 0

    def __call__(self):
        r = self.x
        self.x += 1
        return r


class TestDep(unittest.TestCase):
    def test_dep_0(self):
        d = Deps()
        d.put('a', 'b', 'c', 'd')
        self.assertEqual({'a': {'b', 'c', 'd'}}, d.deps)
        self.assertEqual({'b': {'a'}, 'c': {'a'}, 'd': {'a'}, 'a': set()}, d.deps_rev)

        self.assertEqual(list(), list(d.pending))

        with self.assertRaises(NotCreated):
            d._done('b')
        d.put('b')
        self.assertEqual(['b'], list(d.pending))

        d.put('c')
        d.put('d')
        self.assertEqual(['b', 'c', 'd', 'a'], list(d.pending))
        self.assertEqual({}, d.deps)
        self.assertEqual({}, d.deps_rev)

    def test_dep_1(self):
        d = Deps()
        d.put('a', 'b', 'c', 'd')
        d.put('c', 'e')
        self.assertEqual({'a': {'b', 'c', 'd'}, 'c': {'e'}}, d.deps)
        self.assertEqual({'b': {'a'}, 'c': {'a'}, 'd': {'a'}, 'e': {'c'}, 'a': set()}, d.deps_rev)

        self.assertEqual(list(), list(d.pending))

        d.put('b')
        self.assertEqual(['b'], list(d.pending))

        d.put('d')
        self.assertEqual(['b', 'd'], list(d.pending))
        d.put('e')
        self.assertEqual(({}, {}), (d.deps, d.deps_rev))

    def test_dep_map_0(self):
        deps = KeyedDeps(lambda x: string.ascii_lowercase.index(x))
        deps.put('a', 'b', 'c', 'd', 'e')

        deps.put('e', 'f')

        with self.assertRaises(IndexError):
            deps.pop()

        with self.assertRaises(IndexError):
            self.assertEqual(deps.pop(), ('b', []))

        deps.put('b')
        deps.put('c')
        deps.put('d')

        self.assertEqual(deps.pop(), ('b', []))
        self.assertEqual(deps.pop(), ('c', []))
        self.assertEqual(deps.pop(), ('d', []))

        deps.put('f')

        self.assertEqual(deps.pop(), ('f', []))
        self.assertEqual(deps.pop(), ('e', ['f']))
        self.assertEqual(deps.pop(), ('a', ['b', 'c', 'd', 'e']))
        # self.assertEqual(deps.pop(), ('f', []))

        self.assertEqual({}, deps.values)
        self.assertEqual({}, deps.values_deps)
        self.assertEqual({}, deps.values_deps_rev)

        deps.put('a')

        self.assertEqual(deps.pop(), ('a', []))
