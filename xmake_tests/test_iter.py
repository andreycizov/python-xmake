import unittest

from xmake.dsl import Con, Iter, Eval, Var, With
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
