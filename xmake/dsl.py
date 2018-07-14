import inspect
import logging
import string
from collections import deque
from typing import List, Any, Tuple, Callable, Union, Optional

from dataclasses import dataclass, field

from xmake.abstract import TRes, TPostRes, Ctx, Op, _get_caller
from xmake.error import OpError


@dataclass()
class LeftRightRes:
    left: Any
    right: Any


@dataclass
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


@dataclass
class Var(Op):
    name: VarName

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        try:
            return ctx, ctx.get(self.name)
        except KeyError:
            raise OpError(self, f'No variable mapping found `{self.name}`')


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
class Log(Op):
    node: Op
    name: str = field(default_factory=lambda: __name__)
    msg: Optional[str] = None

    def __init__(self, *args: Union[str, Op]):
        guessed_name = inspect.getmodule(_get_caller(2)[0]).__name__

        *args, node = args

        assert isinstance(node, Op), node

        new_name = None
        name = guessed_name
        msg = None

        if len(args) == 2:
            new_name, msg = args
        elif len(args) == 1:
            msg, = args
        elif len(args) == 0:
            pass
        else:
            raise NotImplementedError('')

        if new_name is not None:
            name = new_name

        self.node = node
        self.name = name
        self.msg = msg

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return [self.node]

    def execute(self, arg: Any) -> TRes:
        logging.getLogger(self.name).warning('[%s] %s', self.msg if self.msg else '_', arg)
        return arg


@dataclass
class Err(Op):
    msg: Optional[str]
    args: List[Op]

    def __init__(self, msg: Optional[str] = None, *args: Op):
        self.msg = msg
        self.args = args

        self.__post_init__()

    def dependencies(self):
        return self.args

    def execute(self, *args: Any) -> TRes:
        msg = self.msg

        if msg is None:
            msg = 'Error'

        msg = msg % args

        raise OpError(self, reason=msg)


@dataclass
class Eval(Op):
    args: List[Op]
    body: Union[str, Callable, Op]

    def __init__(self, *args: Union[str, Callable, Op]):
        for arg in args[:-1]:
            assert isinstance(arg, Op), arg

        body = args[-1]
        assert isinstance(body, (str, Callable, Op))

        self.body = body
        self.args = args[:-1]

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        if isinstance(self.body, Op):
            assert not self.args, self.args
            return [self.body]
        else:
            return self.args

    # eval needs arguments

    def execute(self, *args: Any) -> TRes:
        if isinstance(self.body, Op):
            return args[0]
        elif isinstance(self.body, str):
            r = _eval_map_args(args)
            return eval(self.body, {}, r)
        elif callable(self.body):
            return self.body(*args)
        else:
            raise NotImplementedError(self.body)

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if isinstance(self.body, Op):
            return [result]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if isinstance(self.body, Op):
            return post_result[0]
        else:
            return execute_ret


# fun agg(agg) -> (agg, agg + 1) if

@dataclass
class Iter(Op):
    map: Var
    aggregator: Op
    next_op: Op
    map_op: Op

    def dependencies(self) -> List['Op']:
        # how do we pass an aggregator to the dependency ?
        return [With(self.map, self.aggregator, self.next_op)]

    def execute(self, rtn: Any) -> TRes:
        return rtn

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        item, new_agg = result

        # we need to somehow remember the last item returned.

        if new_agg is not None:
            return [
                Iter(self.map, Con(new_agg), self.next_op, self.map_op),
                With(self.map, Con(item), self.map_op)
            ]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if len(post_result):
            prev_iter, curr_map = post_result
            if prev_iter is not None:
                return prev_iter
            else:
                return curr_map
        else:
            return None


@dataclass
class With(Op):
    vars: List[Var]
    vals: List[Op]
    map_op: Op

    def __init__(self, *args: Union[Var, Op]):
        args = deque(args)
        vars = []
        vals = []
        while len(args) > 1:
            var = args.popleft()
            assert isinstance(var, Var), var
            vars.append(var)
            val = args.popleft()
            assert isinstance(val, Op), val
            vals.append(val)

        try:
            map_op = args.popleft()
        except IndexError:
            raise OpError(self, 'With needs a body')

        assert isinstance(map_op, Op), map_op

        self.vars = vars
        self.vals = vals
        self.map_op = map_op

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return self.vals

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        for var, val in zip(self.vars, args):
            ctx = ctx.push(var.name, val)

        return ctx, None

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        # are dependencies called with the return value of me ?
        return [self.map_op]

    def context_post_execute(self, ctx: Ctx, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> \
            Tuple[Ctx, TPostRes]:
        for var in self.vars:
            ctx = ctx.pop(var.name)

        return ctx, post_result[0]


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

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return [With(self.map, self.value_op, self.cases[0].match_op)]

    def execute(self, val) -> TRes:
        return val

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if result:
            return [With(self.map, self.value_op, self.cases[0].map_op)]
        elif len(self.cases) > 1:
            return [Match(self.map, self.value_op, *self.cases[1:])]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if len(post_result) == 0:
            raise OpError(self, 'unmatched')
        else:
            return post_result[0]


# cps is when one function calls the other function

@dataclass(init=False)
class Fun(Op):
    args: List[Var]
    body: Op

    def __init__(self, *params: Union[Var, Op]):
        *args, body = params

        found = set()
        duplicates = set()

        for arg in args:
            assert isinstance(arg, Var)
            if arg.name not in found:
                if arg.name in found:
                    duplicates.add(arg.name)
                found.add(arg.name)

        if duplicates:
            first = [arg for arg in args if arg.name in duplicates][0]
            raise OpError(first, f'duplicate fun args {duplicates} (marking position for the first one)')

        assert isinstance(body, Op)

        self.args = args
        self.body = body

        self.__post_init__()

    def execute(self, *args: Any):
        return self


@dataclass(init=False)
class Call(Op):
    fun: Op
    args: List[Op]

    def __init__(self, fun: Op, *args: Op):
        self.fun = fun
        self.args = list(args)

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return [self.fun] + self.args

    def execute(self, fun: Fun, *args: Any) -> TRes:
        return fun, args

    def context_post_dependencies(self, ctx: Ctx, result: TRes, *pre_result: List[TRes]) -> Tuple[Ctx, List['Op']]:
        fun, args = result
        assert isinstance(fun, Fun), fun

        not_found = {x.name for x in fun.args}

        for var, val in zip(fun.args, args):
            ctx = ctx.push(var.name, val)
            not_found.remove(var.name)

        if len(not_found):
            not_found = list(not_found)
            raise OpError(self, f'NOT_ENOUGH could not find mappings for variables named {not_found}')

        if len(fun.args) < len(args):
            others = list(zip(self.args, args))[len(fun.args):]
            raise OpError(self, f'TOO_MANY could not find mappings for variables valued {others}')

        return ctx, [fun.body]

    def context_post_execute(self, ctx: Ctx, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> \
            Tuple[Ctx, TPostRes]:
        fun, args = execute_ret
        assert isinstance(fun, Fun), fun

        for var in fun.args:
            ctx = ctx.pop(var.name)
        return ctx, post_result[0]


@dataclass()
class Seq(Op):
    ops: List[Op]

    def __init__(self, *ops: Op):
        self.ops = tuple(ops)

        self.__post_init__()

    def __repr__(self) -> str:
        r = ', '.join(str(x) for x in self.ops)
        return f'{self.__class__.__name__}({r})'

    def dependencies(self) -> List['Op']:
        if len(self.ops):
            return [self.ops[0]]
        else:
            return []

    def execute(self, *ret: Any) -> TRes:
        if len(self.ops):
            return ret[0]
        else:
            return None

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if len(self.ops) > 1:
            return [Seq(*self.ops[1:])]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if len(self.ops) > 1 and post_result[0] is not None:
            return post_result[0]
        else:
            return execute_ret


@dataclass()
class Par(Op):
    ops: List[Op]

    def __init__(self, *ops: Op):
        self.ops = list(ops)

        self.__post_init__()

    def __repr__(self) -> str:
        r = ', '.join(str(x) for x in self.ops)
        return f'{self.__class__.__name__}({r})'

    def dependencies(self) -> List['Op']:
        return self.ops

    def execute(self, *args: Any) -> TRes:
        return list(args)


@dataclass()
class CPS(Op):
    arg: Var
    ret: Var
    ops: List[Op]

    def __init__(self, arg: Var, ret: Var, *ops: Op):
        self.arg = arg
        self.ret = ret
        self.ops = ops

        self.__post_init__()

    def context_dependencies(self, ctx: Ctx):
        Call(Fun(self.arg, self.ret, self.ops[0]), )

        ctx = ctx.push(self.ret.name, Fun(
            self.arg,
            self.ops[0]
        ))
