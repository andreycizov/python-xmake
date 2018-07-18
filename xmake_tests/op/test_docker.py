import unittest

from xmake.dsl import With, Var, Con, Seq
from xmake.executor import Executor
from xmake.op.docker import ImagePull, ContainerStart, ContainerCreate, ContainerConfig, ContainerAttach, \
    ContainerRemove, ImageList


class TestImage(unittest.TestCase):
    def setUp(self):
        pass

    def test_start(self):
        expr = With(
            Con('unix:///var/run/docker.sock'),
            lambda docker: With(
                ImagePull('alpine:3.5'),
                lambda i: With(
                    lambda: ContainerCreate(
                        i,
                        'test_simple',
                        ['sh', '-c', 'echo asd'],
                        ContainerConfig(labels={'test': '1'})
                    ),
                    lambda c: Seq(
                        # notice the difference between dynamically generated steps from bound variables
                        lambda: ContainerStart(c),
                        lambda: ContainerAttach(c),
                        lambda: ContainerRemove(c),
                        # and the return value of the actual variable
                        c
                    )
                )
            )
        )

        r = Executor(should_trace=True).execute(expr)

        print(r)

    def test_image(self):
        expr = With(
            Con('unix:///var/run/docker.sock'),
            lambda docker: Seq(
                ImagePull('alpine:3.5')
            )
        )

        r = Executor(should_trace=True).execute(expr)

        print(
            r
        )

    def tearDown(self):
        pass
