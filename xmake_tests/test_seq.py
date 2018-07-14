import unittest

from xmake.dsl import Seq, Con
from xmake.executor import Executor


def executor(body):
    return Executor().execute(body)


class TestSeq(unittest.TestCase):
    def test_seq_0(self):
        self.assertEqual([1, 2], executor(Seq(Con(1), Con(2))))

    def test_seq_1(self):
        self.assertEqual([1], executor(Seq(Con(1))))

    def test_seq_2(self):
        self.assertEqual([1, 2, 3], executor(Seq(Con(1), Con(2), Con(3))))

    def test_seq_20(self):
        self.assertEqual([1, 2], executor(Seq(Con(1), Con(2))))

    def test_seq_21(self):
        self.assertEqual([1], executor(Seq(Con(1))))

    def test_seq_22(self):
        self.assertEqual([1, 2, 3], executor(Seq(Con(1), Con(2), Con(3))))
