import logging
from typing import Any, Dict, List

from dataclasses import dataclass, field

from xmake.dep import KeyedDeps
from xmake.dsl import Op, Ctx
from xmake.error import ExecError
from xmake.runtime import Step, JOB_STATE_SUCCESSOR, JOB_STATE_PREDECESSOR, JobRecID, JobRec


class LazyLog:
    def __init__(self, callable):
        self.callable = callable

    def __str__(self) -> str:
        return self.callable()


@dataclass
class Counter:
    x: int = 0

    def __call__(self):
        r = self.x
        self.x += 1
        return r


@dataclass()
class Executor:
    should_trace: bool = False
    deps: KeyedDeps[JobRecID, JobRec] = field(default_factory=lambda: KeyedDeps(lambda x: x.id))
    ctr: Counter = field(default_factory=Counter)
    rets: Dict[JobRecID, Any] = field(default_factory=dict)
    reqs: Dict[JobRecID, List[JobRecID]] = field(default_factory=dict)

    def execute(self, root: Op):
        root_ctx = Ctx()
        # we would like the queue to execute the jobs.
        exit_rec = JobRec(self.ctr(), Step.Deps, None, root_ctx)
        root_rec = JobRec(self.ctr(), Step.Deps, root, root_ctx)

        self.reqs[root_rec.id] = [root_rec.with_step(Step.Result).id]

        self.deps.put(exit_rec, root_rec.with_step(Step.Result))
        self.deps.put(root_rec)

        while len(self.deps):
            job_rec, job_deps = self.deps.pop()

            if job_rec.job is None:
                exit_job_dep, = job_deps
                return self.rets[exit_job_dep.id]

            callable_fun = getattr(self, 'execute_' + job_rec.step.value.lower())

            try:
                new_ctx, deps, ret = callable_fun(job_rec, job_deps)
            except Exception as e:
                raise ExecError(job_rec, job_deps, e)

            deps_objs = []

            self.reqs[job_rec.id] = []

            for dep in deps:
                dep_rec = JobRec(self.ctr(), Step.Deps, dep, new_ctx)

                self.deps.put(dep_rec)

                dep_res_rec = dep_rec.with_step(Step.Result)
                deps_objs.append(dep_res_rec)

                self.reqs[job_rec.id].append(dep_res_rec.id)

            self.rets[job_rec.id] = ret

            succ = JOB_STATE_SUCCESSOR.get(job_rec.step)

            if self.should_trace:
                logging.getLogger(__name__).warning('TRACE %s %s %s %s %s', job_rec, deps, ret, succ, deps_objs)
                logging.getLogger(__name__).warning('CTX %s', new_ctx)

            if succ:
                self.deps.put(job_rec.with_step(succ).with_ctx(new_ctx), *deps_objs)
            else:
                assert len(deps_objs) == 0, deps_objs

            pred = JOB_STATE_PREDECESSOR.get(job_rec.step)

            if pred:
                pred_job_rec = job_rec.with_step(pred)

                # todo cleanup
                continue

                del self.rets[pred_job_rec]

    def execute_deps(self, job_rec: JobRec, job_deps: List[JobRec]):
        ctx, deps = job_rec.job.context_dependencies(job_rec.ctx)

        return ctx, deps, deps

    def execute_exec(self, job_rec: JobRec, job_deps: List[JobRec]):
        ctx, ret = job_rec.job.context_execute(
            job_rec.ctx,
            *self._deps_res(job_rec)
        )

        return ctx, [], ret

    def _deps_res(self, job_rec: JobRec, step=Step.Deps):
        return [self.rets[j] for j in self.reqs[job_rec.with_step(step).id]]

    def execute_postdeps(self, job_rec: JobRec, job_deps: List[JobRec]):
        ctx, deps = job_rec.job.context_post_dependencies(
            job_rec.ctx,
            self.rets[job_rec.with_step(Step.Exec).id],
            *self._deps_res(job_rec)
        )

        return ctx, deps, deps

    def execute_postexec(self, job_rec: JobRec, job_deps: List[JobRec]):
        ctx, ret = job_rec.job.context_post_execute(
            job_rec.ctx,
            self.rets[job_rec.with_step(Step.Exec).id],
            self._deps_res(job_rec),
            self._deps_res(job_rec, Step.PostDeps),
        )

        return ctx, [], ret

    def execute_result(self, job_rec: JobRec, job_deps: List[JobRec]):
        ctx, ret = job_rec.ctx, self.rets[job_rec.with_step(Step.PostExec).id]
        return ctx, [], ret
