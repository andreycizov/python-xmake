import unittest

from xmake.dsl import Con, Iter, Eval, Var, With, Match, Case
from xmake.executor import Executor


class TestDSL(unittest.TestCase):
    def test_seq_0(self):
        # assume a variable generated is always unique.
        # a context.

        # a) save all of the variable mappings and then pass them down the context
        # b) allow only global variables and therefore allow to directly depend upon them
        # c)

        prog = Iter(
            0,
            Var('id'),
            Eval('a + 1 if a < 10 else None', Var('id')),
            With(
                Var('a'),
                Eval('a + 1 if a < 10 else None', Var('id')),
                Eval('0', Var('a')),
            )
        )
        self.assertEqual(None, Executor().execute(prog))

    def test_with(self):
        ex = Executor()
        r = ex.execute(
            With(
                Var('a'),
                Con(5),
                Eval('5 + 1', Var('a'))
            )
        )

        self.assertEqual(6, r)

        # todo
        #self.assertEqual(ex.reqs, {})
        #self.assertEqual(ex.rets, {})

    def test_match_0(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(lambda x: x == 5, Var('x')), Con('a')),
                Case(Eval(lambda x: x == 5, Var('x')), Con('b')),
                Case(Eval(lambda x: x == 1, Var('x')), Con('c')),
            )
        )

        self.assertEqual('a', r)

    def test_match_1(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(lambda x: x == 1, Var('x')), Con('a')),
                Case(Eval(lambda x: x == 5, Var('x')), Con('b')),
                Case(Eval(lambda x: x == 1, Var('x')), Con('c')),
            )
        )

        self.assertEqual('b', r)

    def test_match_2(self):
        ex = Executor()

        r = ex.execute(
            Match(
                Var('x'),
                Con(5),
                Case(Eval(lambda x: x == 1, Var('x')), Con('a')),
                Case(Eval(lambda x: x == 2, Var('x')), Con('b')),
                Case(Eval(lambda x: x == 5, Var('x')), Con('c')),
            )
        )

        self.assertEqual('c', r)


