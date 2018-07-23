from enum import Enum

from dataclasses import dataclass, replace
from typing import Tuple, Optional

from xmake.dsl import Op, Ctx


class OrderedEnum(Enum):
    @property
    def _order(self):
        return list(self.__class__).index(self)

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self._order >= other._order
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self._order > other._order
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self._order <= other._order
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self._order < other._order
        return NotImplemented


class Step(OrderedEnum):
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


@dataclass(frozen=True)
class JobRecID:
    ident: int
    step: Step

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ident}, {self.step})'

    def with_step(self, step: Step) -> 'JobRec':
        return replace(self, step=step)


@dataclass()
class JobRec:
    ident: int
    step: Step
    job: Optional[Op]
    ctx: Ctx

    @property
    def id(self) -> JobRecID:
        return JobRecID(self.ident, self.step)

    def with_step(self, step: Step) -> 'JobRec':
        return replace(self, step=step)

    def with_ctx(self, ctx: Ctx) -> 'JobRec':
        return replace(self, ctx=ctx)
