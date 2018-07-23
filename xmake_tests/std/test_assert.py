import unittest

from xmake.executor import Executor
from xmake.std import assert_single


class TestAssert(unittest.TestCase):
    def test_assert_single_0(self):
        expr = assert_single([1])
        ret = Executor().execute(expr)

        self.assertEqual(1, ret)

    def test_assert_single_1(self):
        expr = assert_single([1, 2])
        ret = Executor().execute(expr)

        self.assertEqual(1, ret)
