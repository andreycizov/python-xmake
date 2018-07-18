import unittest

from xmake.dsl import Seq, Fun, Match, Case, Err, With, Con, OpError
from xmake.error import ExecError
from xmake.executor import Executor


class TestDSLExt(unittest.TestCase):
    def test_fun(self):
        ex = Fun(
            # we need to understand that lambdas are executed immediately upon object construction,
            # so there would be no real hell in debugging this
            lambda x, y, z: Seq(
                x,
                y,
                z
            )
        )

        ex = ex('a', 'b', 'c')

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual('c', ret)

    def test_match(self):
        ex = Match(
            'a',  # anything that is not a variable must be matched to a Con
            lambda m: [
                Case(
                    m.len > 1,
                    Err('Can not be longer than 1 %s', m),
                ),
                Case(
                    m.len == 1,
                    m[0],
                ),
                Case(
                    m.len == 0,
                    Err('Can not be empty %s', m),
                ),
            ]
        )

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual(ret, 'a')

    def test_with(self):
        ex = With(
            'a',  # anything that is not a variable must be matched to a Con
            'b',
            'c',
            lambda x, y, z: Seq(
                x + y + z
            )
        )

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual('abc', ret)

    def test_callable_00(self):
        ex = With(
            'a',
            'b',
            'c',
            lambda x, y, z: Seq(
                lambda: x
            )
        )

        try:
            ret = Executor(should_trace=True).execute(ex)
        except ExecError as e:
            self.assertIsInstance(e.e, OpError)
            self.assertEquals('`a` returned to Eval is not an Op', e.e.reason)

    def test_callable_01(self):
        ex = With(
            'a',
            'b',
            'c',
            lambda x, y, z: Seq(
                lambda: Con(x)
            )
        )

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual('a', ret)

    def test_callable_1(self):
        ex = With(
            'a',
            'b',
            'c',
            lambda x, y, z: Seq(
                With(
                    'd',
                    lambda d: Seq(lambda: Con(d))
                )
            )
        )

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual('d', ret)

    def test_callable_wrapped_0(self):
        ex = With(
            'a',
            'b',
            'c',
            lambda x, y, z: With(
                lambda: Con(x),
                lambda w: w
            )
        )

        ret = Executor(should_trace=True).execute(ex)

        self.assertEqual('a', ret)
