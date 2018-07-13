import logging
from collections import deque
from enum import Enum
from typing import Dict, List, Set, Any, Deque, Tuple

from dataclasses import dataclass, field

from xmake.dsl import Op


class JobState(Enum):
    Deps = 'Deps'
    Exec = 'Exec'
    PostDeps = 'PostDeps'
    PostExec = 'PostExec'
    Result = 'Result'

    def __repr__(self):
        return self.value


OpResult = Tuple[JobState, Op]


@dataclass()
class DependencyIndex:
    deps: Dict[OpResult, Set[OpResult]] = field(default_factory=dict)
    deps_cache: Dict[OpResult, List[OpResult]] = field(default_factory=dict)
    deps_rev: Dict[OpResult, Set[OpResult]] = field(default_factory=dict)
    deps_rev_cache: Dict[OpResult, Set[OpResult]] = field(default_factory=dict)
    results: Dict[OpResult, Any] = field(default_factory=dict)
    queue: Deque[OpResult] = field(default_factory=deque)

    def depends_on(self, op: OpResult) -> List[OpResult]:
        return self.deps.get(op, [])

    def depended_on(self, op: OpResult) -> List[OpResult]:
        return list(self.deps_rev[op])

    def all_known(self, op: OpResult):
        return self.deps.get(op), self.deps_cache.get(op), self.deps_rev.get(op), self.deps_rev_cache.get(op)

    def add_depends(self, a: OpResult, b: OpResult):
        # todo assert there are no deps assigned to tasks that are in results
        if a not in self.deps_cache:
            # self.queue.append(b)
            self.deps[a] = set()
            self.deps_cache[a] = []
        if a not in self.deps:
            self.deps[a] = set()

        if b not in self.results:
            self.deps[a].add(b)
        self.deps_cache[a].append(b)

        if b not in self.deps_rev_cache:
            # todo: this is a temp fix.
            self.queue.appendleft(b)
            self.deps_rev[b] = set()
            self.deps_rev_cache[b] = set()
        if b not in self.deps_rev:
            self.deps_rev[b] = set()

        if b not in self.results:
            self.deps_rev[b].add(a)
        self.deps_rev_cache[b].add(a)

    def _deps_changed(self, a: OpResult, b: OpResult):
        pass

    def dependency_results(self, item: OpResult) -> List[Any]:
        # what are the results of the items I depend on?

        depends_on = self.depends_on(item)
        assert len(depends_on) == 0, (item, depends_on)

        return [self.results[x] for x in self.deps_cache.get(item, [])]

    def resolve_depends(self, item: OpResult, result: Any) -> List[OpResult]:
        # an items has a result now.

        logging.getLogger(__name__ + '.resolve_depends').debug('%s %s', item, result)
        self.results[item] = result

        resolved = []
        for x in self.deps_rev.get(item, []):
            self.deps[x].remove(item)
            if len(self.deps.get(x, [])) == 0:
                resolved.append(x)
                del self.deps[x]

        if item in self.deps_rev:
            del self.deps_rev[item]

        # todo for each item in resolved, push it into the queue.

        return resolved
