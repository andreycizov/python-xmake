from collections import deque
from typing import List, Set, Dict, TypeVar, Generic, Deque, Optional, Callable

from dataclasses import dataclass, field

JobID = TypeVar('JobID')
Job = TypeVar('Job')


class NotCreated(Exception):
    pass


class Exists(Exception):
    pass


@dataclass()
class Deps(Generic[JobID]):
    deps: Dict[JobID, Set[JobID]] = field(default_factory=dict, repr=False)
    deps_rev: Dict[JobID, Set[JobID]] = field(default_factory=dict, repr=False)

    pending: Deque[JobID] = field(default_factory=deque, repr=False)

    def _maybe_free(self, job: JobID):
        if len(self.deps[job]) == 0:
            self.pending.append(job)

            self._done(job)

    def _done(self, job: JobID):
        if job not in self.deps:
            raise NotCreated(job)

        for dep in self.deps_rev.get(job, set()):
            self.deps[dep].remove(job)
            self._maybe_free(dep)

        del self.deps_rev[job]
        del self.deps[job]

    def put(self, job: JobID, *deps: JobID):
        if job in self.deps:
            raise Exists(job, self.deps)

        self.deps[job] = set()

        if job not in self.deps_rev:
            self.deps_rev[job] = set()

        for x in deps:
            self.deps[job].add(x)

            if x not in self.deps_rev:
                self.deps_rev[x] = set()

            self.deps_rev[x].add(job)

        self._maybe_free(job)

    def peek(self) -> Optional[JobID]:
        if len(self.pending):
            return self.pending[0]
        else:
            return None

    def pop(self) -> JobID:
        return self.pending.popleft()


@dataclass()
class KeyedDeps(Generic[JobID, Job]):
    job_id_fun: Callable[[Job], JobID]
    deps: Deps[JobID] = field(default_factory=Deps)
    values: Dict[JobID, Job] = field(default_factory=dict)
    values_deps: Dict[JobID, List[JobID]] = field(default_factory=dict)
    values_deps_rev: Dict[JobID, Set[JobID]] = field(default_factory=dict)

    def put(self, job: Job, *deps: Job):
        job_id = self.job_id_fun(job)

        dep_ids = [self.job_id_fun(x) for x in deps]

        self.values[job_id] = job
        self.values_deps[job_id] = []

        if job_id not in self.values_deps_rev:
            self.values_deps_rev[job_id] = set()

        for dep_job_id, dep_job in zip(dep_ids, deps):
            self.values[dep_job_id] = dep_job

            if dep_job_id not in self.values_deps_rev:
                self.values_deps_rev[dep_job_id] = set()

            self.values_deps_rev[dep_job_id].add(job_id)
            self.values_deps[job_id].append(dep_job_id)

        self.deps.put(job_id, *dep_ids)

    def peek(self) -> Optional[JobID]:
        return self.deps.peek()

    def _maybe_gc(self, job_id):
        if len(self.values_deps_rev[job_id]):
            return

        del self.values_deps_rev[job_id]
        del self.values[job_id]

    def pop(self):
        # popping a value should allow someone
        # a popped job does not have any dependencies

        job_id = self.deps.pop()
        job = self.values[job_id]
        job_deps = [self.values[x] for x in self.values_deps[job_id]]

        for dep_id in self.values_deps[job_id]:
            self.values_deps_rev[dep_id].remove(job_id)

            self._maybe_gc(dep_id)

        del self.values_deps[job_id]

        self._maybe_gc(job_id)

        return job, job_deps

    def __len__(self):
        return len(self.values)
