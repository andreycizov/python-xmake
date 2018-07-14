import unittest

from xmake.dsl import Seq, Con, Par
from xmake.executor import Executor


def executor(body):
    return Executor(should_trace=True).execute(body)


class TestSeq(unittest.TestCase):
    def test_seq_0(self):
        self.assertEqual(None, executor(Seq()))

    def test_seq_1(self):
        self.assertEqual(1, executor(Seq(Con(1))))

    def test_seq_2(self):
        self.assertEqual(2, executor(Seq(Con(1), Con(2))))

    def test_seq_3(self):
        self.assertEqual(3, executor(Seq(Con(1), Con(2), Con(3))))

    def test_par_0(self):
        self.assertEqual([], executor(Par()))

    def test_par_1(self):
        self.assertEqual([1], executor(Par(Con(1))))

    def test_par_2(self):
        self.assertEqual([1, 2], executor(Par(Con(1), Con(2))))

    def test_par_3(self):
        self.assertEqual([1, 2, 3], executor(Par(Con(1), Con(2), Con(3))))
