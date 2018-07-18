import unittest
from time import sleep

from xmake.dsl import With, Con, Seq, Match, Err, Case, Fil, Map, Eval
from xmake.executor import Executor
from xmake.op.docker import ImagePull, ContainerStart, ContainerCreate, ContainerConfig, ContainerRemove, ContainerList, \
    ContainerLogs, ContainerWait


class TestImage(unittest.TestCase):
    def setUp(self):
        Executor(should_trace=True).execute(
            With(
                'unix:///var/run/docker.sock',
                lambda docker: Map(
                    lambda x: Seq(
                        lambda: ContainerRemove(x)
                    ),
                    Fil(
                        lambda x: x,
                        ContainerList(filters={'label': 'test'})
                    )
                )
            )
        )

    def test_start(self):
        cmd = ['sh', '-c', 'echo asd']
        expr = With(
            'unix:///var/run/docker.sock',
            lambda docker: With(
                ImagePull('alpine:3.5'),
                lambda i: With(
                    lambda: ContainerCreate(
                        i,
                        'test_simple',
                        cmd,
                        ContainerConfig(labels={'test': '1'})
                    ),
                    lambda c: Seq(
                        With(
                            Match(
                                lambda: Map(
                                    lambda x: x,
                                    Fil(
                                        lambda x: x['Names'][0] == '/test_simple',
                                        ContainerList(),

                                    )
                                ),
                                lambda m: [
                                    Case(
                                        m.len == 1,
                                        m[0]
                                    ),
                                    Case(
                                        True,
                                        Err('Wrong number of containers: %s', m.len)
                                    ),
                                ]
                            ),
                            lambda container: Match(
                                container,
                                lambda m: [
                                    Case(
                                        m['Command'] == cmd,
                                        m
                                    ),
                                    Case(
                                        True,
                                        Err('Command is incorrect: "%s"', m)
                                    )
                                ]
                            )
                        ),
                        # notice the difference between dynamically generated steps from bound variables
                        lambda: ContainerStart(c),
                        lambda: Seq(
                            lambda: Eval(lambda: sleep(1)),
                        ),
                        # todo this will hang if the container managed to stop before it's called
                        lambda: ContainerWait(c),
                        lambda: ContainerLogs(c),
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
        self.setUp()
