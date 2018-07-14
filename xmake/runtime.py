from enum import Enum
from typing import Tuple, Optional

from dataclasses import dataclass, replace

from xmake.abstract import Ctx, Op


class Step(Enum):
    Deps = 'Deps'
    Exec = 'Exec'
    PostDeps = 'PostDeps'
    PostExec = 'PostExec'
    Result = 'Result'

    def __repr__(self):
        return self.value


JOB_STATE_SUCCESSOR = {
    Step.Deps: Step.Exec,
    Step.Exec: Step.PostDeps,
    Step.PostDeps: Step.PostExec,
    Step.PostExec: Step.Result,
    Step.Result: None,
}
JOB_STATE_PREDECESSOR = {v: k for k, v in JOB_STATE_SUCCESSOR.items()}


JobRecID = Tuple[int, Step]


@dataclass()
class JobRec:
    ident: int
    step: Step
    job: Optional[Op]
    ctx: Ctx

    @property
    def id(self) -> JobRecID:
        return self.ident, self.step

    def with_step(self, step: Step) -> 'JobRec':
        return replace(self, step=step)

    def with_ctx(self, ctx: Ctx) -> 'JobRec':
        return replace(self, ctx=ctx)