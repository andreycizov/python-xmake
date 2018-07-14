import unittest

from xmake.dsl import Eval, Con, Log
from xmake_tests.test_seq import executor


class TestEval(unittest.TestCase):
    def test_eval(self):
        self.assertEqual(5, executor(Eval(Con(2), Con(3), 'a + b')))
        self.assertEqual(5, executor(Eval(Con(2), Con(3), lambda a, b: a + b)))
        self.assertEqual(5, executor(Eval(Log(Eval(Con(2), Con(3), lambda a, b: Con(a + b))))))
