import logging
from pprint import pformat
from typing import Optional

from dataclasses import dataclass

from xmake.dsl import Op
from xmake.struct import DependencyIndex, JobState


class LazyLog:
    def __init__(self, callable):
        self.callable = callable

    def __str__(self) -> str:
        return self.callable()


@dataclass()
class Executor:
    deps: Optional[DependencyIndex] = None
    debug_logging = False

    def push_dep(self, state, item, x):
        self.deps.add_depends((state, item), (JobState.Result, x))
        self.deps.add_depends((JobState.Result, x), (JobState.PostExec, x))
        self.deps.add_depends((JobState.PostExec, x), (JobState.PostDeps, x))
        self.deps.add_depends((JobState.PostDeps, x), (JobState.Exec, x))
        self.deps.add_depends((JobState.Exec, x), (JobState.Deps, x))

    def execute(self, root: Op):
        # how do we enable contexts ?
        self.deps = DependencyIndex()

        self.push_dep(JobState.Result, None, root)
        # todo this is a temp fix.
        self.deps.queue.append((JobState.Result, None))

        # todo run while we've got items in the queue and there are items marked as running
        # todo if there are no items in the queue, no items are running, then there are dependency cycles

        while len(self.deps.queue):
            state, item = self.deps.queue[0]

            if self.debug_logging:
                logging.getLogger(__name__ + '.ep').debug('%s %s', state, item)
                logging.getLogger(__name__ + '.queue').debug(
                    '%s',
                    # state == JobState.Exec,
                    LazyLog(lambda: pformat(list(self.deps.queue))),
                    # list(deps.queue),

                )

                logging.getLogger(__name__ + '.results').debug(
                    '%s',
                    # state == JobState.Exec,
                    LazyLog(lambda: pformat(sorted([(k, v) for k, v in self.deps.results.items()],
                                                   key=lambda x: (x[0][0].value, x[0][1].__class__.__name__)))),

                )

                logging.getLogger(__name__ + '.deps').debug(
                    '%s',
                    # state == JobState.Exec,
                    LazyLog(lambda: '\n' + '\n'.join(repr(x) + ': ' + repr(y) for x, y in (
                        sorted([(k, v) for k, v in self.deps.deps_cache.items()],
                               key=lambda x: (x[0][0].value, x[0][1].__class__.__name__)))))

                    # list(deps.queue),

                )

            if item is None:
                assert len(self.deps.queue) == 1, self.deps.queue
                return self.deps.dependency_results((JobState.Result, None))[0]

            ret = None

            attr = getattr(self, 'execute_' + state.value.lower(), None)

            if attr:
                ret = attr(*self.deps.queue.popleft())
            else:
                raise NotImplementedError(f"Can't do anything here: {state} {top}")

            logging.getLogger(__name__ + '.op').debug('%s %s %s', state, item, ret)

            self.deps.resolve_depends((state, item), ret)

    def execute_deps(self, state, item):
        item_deps = item.dependencies()

        for x in item_deps:
            self.push_dep(JobState.Exec, item, x)

        return item_deps

    def execute_exec(self, state, top):
        # todo only exec and postexec are expected to be run in a separate process.
        deps_on = self.deps.depends_on((JobState.Exec, top))
        if len(deps_on):
            raise NotImplementedError(f"Can't do anything here [1]: {state} {top} {deps_on}")
        top_dep_rets = self.deps.dependency_results((JobState.Exec, top))[1:]
        logging.getLogger(__name__ + '.dep_rets').debug('%s', top_dep_rets)
        top_ret = top.execute(*top_dep_rets)
        logging.getLogger(__name__ + '.ret').debug('%s', top_ret)

        return top_ret

    def execute_postdeps(self, state, item):
        # ret_prev = deps.dependency_results((JobState.Deps, item))
        ret = self.deps.dependency_results((JobState.Exec, item))[:1]

        logging.getLogger(__name__ + '.post_dep_rets').debug('%s', ret)

        post_deps = item.post_dependencies(*ret)

        logging.getLogger(__name__ + '.post_dep_rets_deps').debug('%s', post_deps)

        for x in post_deps:
            self.push_dep(JobState.PostExec, item, x)

        return post_deps

    def execute_postexec(self, state, item):
        # todo only exec and postexec are expected to be run in a separate process.

        ret_prev = self.deps.dependency_results((JobState.PostDeps, item))
        post_deps, *ret = self.deps.dependency_results((JobState.PostExec, item))
        # post_deps is a result returned by execute_postdeps (list of dependencies)
        # what is ret ? there will be as many rets as there are post_deps

        logging.getLogger(__name__).getChild('execute_postexec').debug('%s', ret)

        ret = item.post_execute(ret_prev, ret)

        return ret

    def execute_result(self, state, item):
        dep_ret = self.deps.dependency_results((JobState.Result, item))
        ret = dep_ret[-1]

        logging.getLogger(__name__).getChild('execute_result').debug('%s', dep_ret)

        resolved = self.deps.resolve_depends((JobState.Result, item), ret)

        return ret
