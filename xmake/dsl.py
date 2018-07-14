import logging
import string
from typing import TypeVar, List, Any, Tuple, Callable, Union

from dataclasses import dataclass, field

TRes = TypeVar('TRes')
TPostRes = TypeVar('TPostRes')


@dataclass()
class Ctx:
    mappings: List[Tuple[str, Any]] = field(default_factory=list)

    def get(self, n: str):
        for cn, cv in self.mappings[::-1]:
            if cn == n:
                return cv
        else:
            raise KeyError(n)

    def push(self, n: str, val: Any) -> 'Ctx':
        return Ctx(self.mappings + [(n, val)])

    def pop(self, n: str) -> Any:
        idxs = range(len(self.mappings) - 1, -1, -1)
        for idx, (cn, cv) in ((idx, self.mappings[idx]) for idx in idxs):
            if cn == n:
                return Ctx(self.mappings[:idx] + self.mappings[idx + 1:])
        else:
            raise KeyError(n)


class Op:
    # how is an operation allowed to manipulate contexts ?

    @property
    def logger(self):
        return logging.getLogger(self.__module__ + '.' + self.__class__.__name__)

    def context_dependencies(self, ctx: Ctx) -> Tuple[Ctx, List['Op']]:
        """
        :return: list of dependencies to execute before ``execute``
        """
        return ctx, self.dependencies()

    def dependencies(self) -> List['Op']:
        """
        :return: list of dependencies to execute before ``execute``
        """
        return []

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        res = self.execute(*args)
        return self.context_enter(ctx, res, *args), res

    def execute(self, *args: Any) -> TRes:
        """
        :param args: what is returned by every dependency returned by ``dependencies``
        :return:
        """
        self.logger.debug('%s', args)
        return None

    def context_enter(self, ctx: Ctx, res: TRes, *args: Any) -> Ctx:
        return ctx

    def context_post_dependencies(self, ctx: Ctx, result: TRes, *pre_result: List[TRes]) -> Tuple[Ctx, List['Op']]:
        """
        :param result: what is returned by ``execute``
        :param pre_result: what is returned by ``execute`` for every dependency returned by ``dependencies``
        :return: list of dependencies to execute before ``post_execute``
        """
        return ctx, self.post_dependencies(result, pre_result)

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        """
        :param result: what is returned by ``execute``
        :param pre_result: what is returned by ``execute`` for every dependency returned by ``dependencies``
        :return: list of dependencies to execute before ``post_execute``
        """
        return []

    def context_post_execute(
            self,
            ctx: Ctx,
            execute_ret: TRes,
            pre_result: List[TRes],
            post_result: List[TPostRes]
    ) -> Tuple[Ctx, TPostRes]:
        ret = self.post_execute(execute_ret, pre_result, post_result)

        return self.context_exit(ctx, ret, pre_result, post_result), ret

    def context_exit(self, ctx: Ctx, ret: TPostRes, pre_result: List[TRes], post_result: List[TPostRes]) -> Ctx:
        return ctx

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        """
        :param execute_ret: what is returned by ``execute``
        :param pre_result: what is returned by every dependency returned by ``dependencies``
        :param post_result: what is returned by every dependency returned by ``post_dependencies``
        :return:
        """
        self.logger.getChild('post_execute').debug('%s %s', pre_result, post_result)
        return execute_ret


@dataclass()
class LeftRightRes:
    left: Any
    right: Any


@dataclass(frozen=True)
class Con(Op):
    value: Any

    def execute(self, *args: Any) -> TRes:
        return self.value


@dataclass
class DependsOn(Op):
    pre: List[Op]
    post: List[Op]

    def dependencies(self) -> List['Op']:
        return self.pre

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        return self.post

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return LeftRightRes(pre_result, post_result)


VarName = str


@dataclass(frozen=True)
class Var(Op):
    name: VarName

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        return ctx, ctx.get(self.name)


EVAL_DICT = string.ascii_lowercase


def _eval_map_args(iter_obj):
    def _map_dict(x):
        r = ''
        while True:
            next_idx = x % len(EVAL_DICT)
            r += EVAL_DICT[next_idx]
            x //= len(EVAL_DICT)

            if x == 0:
                break
        return r

    r1, r2 = {}, {}
    for i, v in enumerate(iter_obj):
        r1[_map_dict(i)] = v
        r2[f'x{i}'] = v
    return {**r1, **r2}


@dataclass
class Eval(Op):
    body: Union[str, Callable]

    args: List[Op]

    def __init__(self, body, *args: Var):
        self.body = body
        self.args = args

    def dependencies(self) -> List['Op']:
        return self.args

    # eval needs arguments

    def execute(self, *args: Any) -> TRes:
        if isinstance(self.body, str):
            r = _eval_map_args(args)
            return eval(self.body, {}, r)
        elif callable(self.body):
            return self.body(*args)
        else:
            raise NotImplementedError('')


@dataclass(frozen=True)
class Iter(Op):
    aggregator: Any
    map: Var
    next_op: Op
    map_op: Op

    def dependencies(self) -> List['Op']:
        # how do we pass an aggregator to the dependency ?
        return [With(self.map, Con(self.aggregator), self.next_op)]

    def execute(self, rtn: Any) -> TRes:
        return rtn

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if result:
            return [Iter(result, self.map, self.next_op, With(self.map, Con(self.aggregator), self.map_op))]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return None


@dataclass(frozen=True)
class With(Op):
    var: Var
    value_op: Op
    map_op: Op

    def dependencies(self) -> List['Op']:
        return [self.value_op]

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        arg, = args
        return ctx.push(self.var.name, arg), None

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        # are dependencies called with the return value of me ?
        return [self.map_op]

    def context_post_execute(self, ctx: Ctx, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> \
            Tuple[Ctx, TPostRes]:
        return ctx.pop(self.var.name), post_result[0]


@dataclass
class Case:
    match_op: Op
    map_op: Op


@dataclass(init=False)
class Match(Op):
    map: Var
    value_op: Op
    cases: List[Case]

    def __init__(self, map: Var, value_up: Op, case: Case, *args: Case):
        self.map = map
        self.value_op = value_up
        self.cases = [case] + list(args)

    def dependencies(self) -> List['Op']:
        return [With(self.map, self.value_op, self.cases[0].match_op)]

    def execute(self, val) -> TRes:
        return val

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if result:
            return [With(self.map, self.value_op, self.cases[0].map_op)]
        elif len(self.cases):
            return [Match(self.map, self.value_op, *self.cases[1:])]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if len(post_result) == 0:
            return None
        else:
            return post_result[0]


@dataclass(init=False)
class Fun(Op):
    args: List[Var]
    body: Op

    def __init__(self, *params: Union[Var, Op]):
        *args, body = params

        for arg in args:
            assert isinstance(arg, Var)

        assert isinstance(body, Op)

        self.args = args
        self.body = body

    def execute(self, *args: Any):
        return self


@dataclass(init=False)
class Call(Op):
    fun: Op
    args: List[Op]

    def __init__(self, fun: Op, *args: Op):
        self.fun = fun
        self.args = list(args)

    def dependencies(self) -> List['Op']:
        return [self.fun] + self.args

    def execute(self, fun: Fun, *args: Any) -> TRes:
        return fun, args

    def context_post_dependencies(self, ctx: Ctx, result: TRes, *pre_result: List[TRes]) -> Tuple[Ctx, List['Op']]:
        fun, args = result
        assert isinstance(fun, Fun), fun

        for var, val in zip(fun.args, args):
            ctx = ctx.push(var.name, val)

        return ctx, [fun.body]

    def context_post_execute(self, ctx: Ctx, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> \
    Tuple[Ctx, TPostRes]:
        fun, args = execute_ret
        assert isinstance(fun, Fun), fun

        for var in fun.args:
            ctx = ctx.pop(var.name)
        return ctx, post_result[0]


@dataclass(unsafe_hash=True, init=False, repr=False)
class Seq(Op):
    ops: List[Op]

    def __init__(self, *ops: Op):
        self.ops = tuple(ops)

    def __repr__(self) -> str:
        r = ', '.join(str(x) for x in self.ops)
        return f'{self.__class__.__name__}({r})'

    def dependencies(self) -> List['Op']:
        return [self.ops[0]]

    def execute(self, ret: Any) -> TRes:
        return ret

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if len(self.ops) > 1:
            return [Seq(*self.ops[1:])]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return [execute_ret] + (post_result[0] if len(self.ops) > 1 else [])
