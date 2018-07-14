import unittest

from xmake.dsl import With, Var, Con, Seq
from xmake.executor import Executor
from xmake.op.docker import ImagePull


class TestImage(unittest.TestCase):
    def test_image(self):
        expr = With(
            Var('docker'),
            Con('unix:///var/run/docker.sock'),
            Seq(
                ImagePull('alpine:3.5'),
            )
        )

        r = Executor(should_trace=True).execute(expr)

        print(
            r
        )
