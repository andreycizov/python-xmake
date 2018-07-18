import unittest

from xmake.dsl import Con, Iter, Eval, Var, With, Match, Case, Fun, Call, Err, OpError, Map, Fil
from xmake.error import ExecError
from xmake.executor import Executor


class TestDSL(unittest.TestCase):
    def test_seq_0(self):
        # assume a variable generated is always unique.
        # a context.

        # a) save all of the variable mappings and then pass them down the context
        # b) allow only global variables and therefore allow to directly depend upon them
        # c)

        prog = Iter(
            Var('id'),
            Con(0),
            Eval(Var('id'), '(a, a + 1) if a < 10 else (a, None)'),
            With(
                Var('a'),
                Eval(Var('id'), 'a + 1 if a < 10 else None'),
                Eval(Var('a'), '0'),
            )
        )
        self.assertEqual(0, Executor(should_trace=True).execute(prog))

    def test_with(self):
        ex = Executor()
        r = ex.execute(
            With(
                Var('a'),
                Con(5),
                Eval(Var('a'), '5 + 1')
            )
        )

        self.assertEqual(6, r)

        # todo
        # self.assertEqual(ex.reqs, {})
        # self.assertEqual(ex.rets, {})

    def test_match_0(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(Var('x'), lambda x: x == 5), Con('a')),
                Case(Eval(Var('x'), lambda x: x == 5), Con('b')),
                Case(Eval(Var('x'), lambda x: x == 1), Con('c')),
            )
        )

        self.assertEqual('a', r)

    def test_match_1(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(Var('x'), lambda x: x == 1), Con('a')),
                Case(Eval(Var('x'), lambda x: x == 5), Con('b')),
                Case(Eval(Var('x'), lambda x: x == 1), Con('c')),
            )
        )

        self.assertEqual('b', r)

    def test_match_2(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(Var('x'), lambda x: x == 1), Con('a')),
                Case(Eval(Var('x'), lambda x: x == 2), Con('b')),
                Case(Eval(Var('x'), lambda x: x == 5), Con('c')),
            )
        )

        self.assertEqual('c', r)

    def test_fun_0(self):
        ex = Executor()

        r = ex.execute(
            With(
                Var('fn'),
                Fun(
                    Var('x'), Var('y'), Var('z'),
                    Eval(Var('x'), Var('y'), Var('z'), lambda x, y, z: [x, y, z]),
                ),
                Call(Var('fn'), Con(5), Con(6), Con(7))
            )
        )

        self.assertEqual([5, 6, 7], r)

    def test_iter_0(self):
        last_items = []

        def last_item_updater(x):
            last_items.append(x)
            return x

        ex = Executor()

        r = ex.execute(
            Iter(
                Var('x'),
                Con(0),
                Eval(Var('x'), lambda x: ((x, x + 1) if x < 5 else (x, None))),
                Eval(Var('x'), last_item_updater),
            )
        )

        self.assertEqual(4, r)

        self.assertEqual([0, 1, 2, 3, 4], last_items)

    def test_err_0(self):
        ex = Executor(should_trace=True)

        try:
            ex.execute(
                Match(
                    Var('m'),
                    Con(5),
                    Case(
                        Eval(Var('m'), lambda m: m == 5),
                        Err('No matching branches found %s', Var('m'))
                    )
                )
            )
        except ExecError as e:
            self.assertIsInstance(e.e, OpError)
            self.assertEquals('No matching branches found 5', e.e.reason)

    def test_attrs_0(self):
        ex = Executor(should_trace=True)

        r = ex.execute(
            Con([1, 2, 3])[1:2]
        )

        self.assertEqual([2], r)

        r = ex.execute(
            Con([1, 2, 3]).len
        )

        self.assertEqual(3, r)

    def test_map_0(self):
        ex = Executor(should_trace=True)

        inp = [1, 2, 3]

        r = ex.execute(
            Map(
                Var('x'),
                Var('x') * Var('x'),
                Con(inp),
            )
        )

        self.assertEqual([x * x for x in inp], r)

    def test_fil_1(self):
        ex = Executor(should_trace=True)

        inp = [1, 2, 3]

        r = ex.execute(
            Fil(
                Var('x'),
                Var('x') > Con(1),
                Con(inp),
            )
        )

        self.assertEqual([x for x in inp if x > 1], r)

    def test_map_err_0(self):
        ex = Executor(should_trace=True)

        try:
            r = ex.execute(
                Map(
                    Var('x'),
                    Var('x') > Con(1),
                    Con(1),
                )
            )
        except ExecError as e:
            self.assertIsInstance(e.e, OpError)
            self.assertEqual('Returned iterable `1` is not a list', e.e.reason)

    def test_fil_err_0(self):
        ex = Executor(should_trace=True)

        try:
            r = ex.execute(
                Fil(
                    Var('x'),
                    Var('x') > Con(1),
                    Con(1),
                    )
            )
        except ExecError as e:
            self.assertIsInstance(e.e, OpError)
            self.assertEqual('Returned iterable `1` is not a list', e.e.reason)

