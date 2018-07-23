import inspect
import logging
import string
from collections import deque

from dataclasses import dataclass, field
from typing import List, Any, Tuple, Callable, Union, Optional, TypeVar

from xmake.util import _get_caller, _enclosed

TRes = TypeVar('TRes')
TPostRes = TypeVar('TPostRes')

WT = Union['Op', Any]


def _wr(x: WT):
    """
    Wrap the given item in a DSL-compatible way; use only in DSL initialisers.

    Please ensure that every call made to _wr has a stack size of 3 max.
    :param x:
    :return:
    """
    if isinstance(x, Op):
        return x
    elif callable(x):
        # if x is a callable, it means it needs to be replaced with Eval parametrised by all of it's closure variables
        # it also can't have any arguments.

        args = _assert_callable(x)
        assert len(args) == 0, args

        for caller_idx in [3]:
            # caller_idx = 3

            caller = _get_caller(caller_idx)[0]

            caller_locals = caller.f_locals
            caller_globals = caller.f_globals
            fn_freevars = x.__code__.co_freevars

            # print('____________________________________________________________')
            #
            # for idx in [1, 2, 3, 4, 5, 6]:
            #     print('a', idx, list(_get_caller(idx)[0].f_locals.keys()))
            #     # print('b', idx,caller_globals)
            #     print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            #
            # import traceback
            #
            # print(''.join(traceback.format_stack()))
            # print('=============================================================')
            try:
                freevars_values = [caller_locals[x] for x in fn_freevars]
            except KeyError:
                continue
            else:
                return Eval(Eval(
                    *freevars_values,
                    _enclosed(x, caller_globals),
                )._with_loc(Loc.from_frame_idx(4)), wrap=True)._with_loc(Loc.from_frame_idx(4))
        else:
            raise KeyError('None')

    else:
        return Con(x)


def _check_callable(fn: Callable):
    """Ops are always callable as part of the DSL"""
    return callable(fn) and not isinstance(fn, Op)


def _assert_callable(fn: Callable):
    spec: inspect.FullArgSpec = inspect.getfullargspec(fn)

    assert spec.varargs is None, spec.varargs
    assert spec.varkw is None, spec.varkw
    assert spec.kwonlyargs is None or spec.kwonlyargs == [], spec.kwonlyargs

    return spec.args


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


@dataclass
class Loc:
    filename: str
    lineno: int

    def __repr__(self):
        return f'Loc("{self.filename}:{self.lineno}")'

    def shift(self, lineno=0):
        return Loc(self.filename, self.lineno + lineno)

    @classmethod
    def from_frame_idx(cls, idx: int = 3):
        return cls.from_frame(_get_caller(idx))

    @classmethod
    def from_frame(cls, fr: inspect.FrameInfo):
        return Loc(fr.filename, fr.lineno)


class OpError(Exception):
    def __init__(self, op: Optional['Op'] = None,
                 reason: Optional[str] = None,
                 loc: Optional[Loc] = None):
        self._loc = loc
        self.op = op
        self.reason = reason
        super().__init__(op, reason)

    @property
    def loc(self):
        if self.op:
            return self.op._loc
        if self._loc:
            return self._loc
        return None

    def __str__(self):
        loc = ''

        if self.loc:
            loc = repr(self.loc)

        items = [loc, repr(self.reason) if self.reason else None, repr(self.op) if self.op else None]

        items = [x for x in items if x]

        items = ', '.join(items)

        return f'{self.__class__.__name__}({items})'


class Operators:
    def __call__(self, *args: 'WT'):
        return Call(self, *args)

    def __getattr__(self, item: 'WT'):
        return GetAttr(self, item)

    def __getitem__(self, item: 'WT'):
        return GetItem(self, item)

    # math

    def __add__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x + y)

    def __sub__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x - y)

    def __mul__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x * y)

    def __truediv__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x / y)

    def __divmod__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: divmod(x, y))

    # binary

    def __and__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x and y)

    def __or__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x or y)

    # sets

    def __contains__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: y in x)

    # comparison

    def __le__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x <= y)

    def __lt__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x < y)

    def __eq__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x == y)

    def __gt__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x > y)

    def __ge__(self, other: 'WT'):
        return Eval(self, other, lambda x, y: x >= y)


@dataclass(repr=False, eq=False)
class Op(Operators):
    def __post_init__(self):
        fr = _get_caller(3)

        loc = Loc.from_frame(fr)

        try:
            self._loc = loc
        except Exception as e:
            raise ValueError(self, fr, loc)

    def _with_loc(self, loc: 'Loc') -> 'Op':
        self._loc = loc
        return self

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'

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
        return execute_ret


class NoVal:
    def __repr__(self):
        return 'NoVal()'

    def __eq__(self, other):
        return isinstance(other, NoVal)


NO_VALUE = NoVal()


@dataclass(repr=False, eq=False)
class GetAttr(Op):
    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({repr(self.value)}, {repr(self.name)})'

    value: Op
    name: Op
    default: Union[NoVal, Op] = NO_VALUE

    def __init__(self, value: WT, name: WT, default: Union[NoVal, WT] = NO_VALUE):
        self.value = _wr(value)
        self.name = _wr(name)
        self.default = _wr(default)

    def dependencies(self) -> List['Op']:
        default = [] if self.default is NO_VALUE else [self.default]
        return [self.value, self.name] + default

    def execute(self, value_res: Any, name_res: Any, *default: Any) -> TRes:
        if name_res == 'len':
            try:
                return len(value_res)
            except TypeError:
                if self.default is not NO_VALUE:
                    return default[0]
                else:
                    raise OpError(self, 'Could not find a length value')
        else:
            if self.default is not NO_VALUE:
                return getattr(value_res, name_res, *default)
            else:
                return getattr(value_res, name_res)


@dataclass(repr=False, eq=False)
class GetItem(Op):
    value: Op
    key: Op

    def __init__(self, value: WT, key: WT):
        self.value = _wr(value)
        self.key = _wr(key)

    def dependencies(self) -> List['Op']:
        return [self.value, self.key]

    def execute(self, value_res: Any, key_res: Any, *default: Any) -> TRes:
        return value_res[key_res]


@dataclass(repr=False, eq=False)
class Con(Op):
    value: Any

    def __repr__(self):
        vr = repr(self.value)
        if len(vr) > 10:
            vr = vr[:10] + '...'
        return f'Con({vr})'

    def execute(self, *args: Any) -> TRes:
        return self.value


VarName = str


@dataclass(repr=False, eq=False)
class Var(Op):
    name: VarName

    def __repr__(self):
        return f'Var({self.name})'

    def context_execute(self, ctx: Ctx, *args: Any) -> Tuple[Ctx, TRes]:
        try:
            return ctx, ctx.get(self.name)
        except KeyError:
            # can this operation return a function that, given
            raise OpError(self, f'No variable mapping found `{self.name}`')


@dataclass(repr=False, eq=False)
class Log(Op):
    """
    Log a node return value using python logging. It has 3 forms.

    .. code-block:: python
        :linenos:

        Log(__name__, '%s', 1),
        Log(__name__, '%s %s', 1, 2),
        Log('%s', 1),
        Log('%s %s', 1, 2),

    .. code-block:: python
        :linenos:

        Log(
            Map(
                lambda x: x * 2,
                [1, 2, 3]
            )
        )

        # >> [xmake.dsl] [_] [2, 4, 6]

    .. code-block:: python
        :linenos:

        Log(
            'list_of_things',
            Map(
                lambda x: x * 2,
                [1, 2, 3]
            )
        )

        # >> [xmake.dsl] [list_of_things] [2, 4, 6]

    .. code-block:: python
        :linenos:

        Log(
            'my_module',
            'list_of_things',
            Map(
                lambda x: x * 2,
                [1, 2, 3]
            )
        )

        # >> [my_module] [list_of_things] [2, 4, 6]

    """

    node: Op
    name: str = field(default_factory=lambda: __name__)
    msg: Optional[str] = None

    def __init__(self, *args: WT):
        guessed_name = inspect.getmodule(_get_caller(2)[0]).__name__

        *args, node = args

        node = _wr(node)

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
        msg = self.msg if self.msg else '_'
        logging.getLogger(self.name).getChild(msg).warning('[%s] %s', msg, arg)
        return arg


@dataclass(repr=False, eq=False)
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


EVAL_DICT = string.ascii_lowercase


def _map_eval_args(iter_obj):
    """Build a locals dict for Eval"""

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


@dataclass(repr=False, eq=False)
class Eval(Op):
    args: List[Op]
    body: Union[str, Callable, Op]
    wrap: bool = False

    def __init__(self, *args: Union[str, Callable, Op], wrap=False):
        body = args[-1]

        self.wrap = wrap

        self.body = body
        self.args = [_wr(x) for x in args[:-1]]

        for arg in self.args:
            assert isinstance(arg, Op), arg

        assert isinstance(body, (str, Callable, Op))

        self.__post_init__()

    def __repr__(self) -> str:
        args = ', '.join(repr(a) for a in self.args + [self.body if isinstance(self.body, Op) else self.body])
        return f'{self.__class__.__name__}({args})'

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
            r = _map_eval_args(args)
            return eval(self.body, {}, r)
        elif callable(self.body):
            return self.body(*args)
        else:
            raise NotImplementedError(self.body)

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if isinstance(self.body, Op):
            if self.wrap:
                result = _wr(result)

            if not isinstance(result, Op):
                raise OpError(self, f'`{result}` returned to Eval is not an Op')

            return [result]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if isinstance(self.body, Op):
            return post_result[0]
        else:
            return execute_ret


# fun agg(agg) -> (agg, agg + 1) if

@dataclass(repr=False, eq=False)
class Iter(Op):
    map: Var
    aggregator: Op
    next_op: Op
    map_op: Op

    def __repr__(self):
        return f'{self.__class__.__qualname__}({repr(self.map)}={repr(self.aggregator)} next={repr(self.next_op)})(<body omitted>)'

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


@dataclass(repr=False, eq=False)
class With(Op):
    vars: List[Var]
    vals: List[Op]
    map_op: Op

    def __repr__(self):
        vals = ', '.join(f'{repr(n)}={repr(v)}' for n, v in zip(self.vars, self.vals))
        body = repr(self.map_op)
        return f'{self.__class__.__qualname__}({vals}|{body})'

    @classmethod
    def from_ext(cls, *args: WT):
        *args, fn = args
        vars = _assert_callable(fn)
        assert len(vars) == len(args), (vars, args)

        ret = []

        for var, val in zip(vars, args):
            ret.append(Var(var))
            ret.append(val)

        ret.append(fn(*[Var(var) for var in vars]))

        return ret

    def __init__(self, *args: WT):
        if _check_callable(args[-1]):
            args = self.from_ext(*args)

        args = deque(args)
        vars = []
        vals = []
        while len(args) > 1:
            var = args.popleft()
            vars.append(var)
            val = args.popleft()
            val = _wr(val)
            vals.append(val)

        try:
            map_op = _wr(args.popleft())
        except IndexError:
            raise OpError(self, 'With needs a body')

        self.vars = vars
        self.vals = vals
        self.map_op = map_op

        for var in self.vars:
            assert isinstance(var, Var), var

        for val in self.vals:
            assert isinstance(val, Op), val

        assert isinstance(self.map_op, Op), self.map_op

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


@dataclass(repr=False, eq=False)
class Case:
    match_op: Op
    map_op: Op

    def __init__(self, match_op: WT, map_op: WT):
        self.match_op = _wr(match_op)
        self.map_op = _wr(map_op)

        assert isinstance(self.match_op, Op), self.match_op
        assert isinstance(self.map_op, Op), self.map_op


@dataclass(repr=False, init=False)
class Match(Op):
    """
    Pattern matching.

    .. code-block:: python
        :linenos:

        Match(
            5,
            lambda m: [
                Case(
                    x > 5,
                    x
                ),
                Case(
                    True,
                    Err('The code had not matched anything', m)
                )
            ]
        )
    """
    map: Var
    value_op: Op
    cases: List[Case]

    @classmethod
    def from_ext(cls, value: WT, fn: Callable):
        args = _assert_callable(fn)

        assert len(args) == 1, args

        map_name, = args
        map = Var(map_name)

        cases = fn(map)

        value_op = value

        return map, value_op, cases

    def __init__(self, *args: WT):
        if _check_callable(args[-1]):
            assert len(args) == 2, args

            map, value_op, cases = self.from_ext(*args)
        else:
            map, value_op, *cases = args

            assert len(cases) >= 1, cases

        self.map = _wr(map)
        self.value_op = _wr(value_op)
        self.cases = cases

        assert isinstance(self.map, Op), self.map
        assert isinstance(self.value_op, Op), self.value_op

        for case in self.cases:
            assert isinstance(case, Case)

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return [With(self.map, self.value_op, self.cases[0].match_op)]

    def execute(self, val) -> TRes:
        return val

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if result:
            return [With(self.map, self.value_op, self.cases[0].map_op)._with_loc(self._loc)]
        elif len(self.cases) > 1:
            return [Match(self.map, self.value_op, *self.cases[1:])._with_loc(self._loc)]
        else:
            return []

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        if len(post_result) == 0:
            raise OpError(self, 'unmatched')
        else:
            return post_result[0]


@dataclass(repr=False, init=False)
class Fun(Op):
    """
    Define a lambda function

    .. code-block:: python
        :linenos:

        Fun(
            lambda a, b, c: Seq(
                a,
                b,
                c
            )
        )

    """

    args: List[Var]
    body: Op

    @classmethod
    def from_ext(cls, fn: Callable):
        rtn_parms = []

        args = _assert_callable(fn)

        for arg in args:
            arg = Var(arg)

            rtn_parms.append(arg)

        fun_body = fn(*rtn_parms)

        assert isinstance(fun_body, Op)

        return rtn_parms + [fun_body]

    def __init__(self, *params: WT):
        if _check_callable(params[-1]):
            assert len(params) == 1
            params = self.from_ext(*params)

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


@dataclass(repr=False, init=False)
class Call(Op):
    # todo call is responsible for currying.!

    fun: Op
    args: List[Op]

    def __init__(self, fun: WT, *args: WT):
        self.fun = _wr(fun)
        self.args = [_wr(x) for x in args]

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


class Seq(Op):
    """
    Sequentially execute a list of operations

    .. code-block:: python
        :linenos:

        Set(
            Log('a', 1),
            Log('b', 2),
            Log('c', 3),
        )
    """

    ops: List[Op]

    def __init__(self, *ops: WT):
        wr_ops = []

        for op in ops:
            wr_ops.append(_wr(op))

        self.ops = wr_ops

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


class Par(Op):
    ops: List[Op]

    def __init__(self, *ops: WT):
        if len(ops):
            if _check_callable(ops[0]):
                fn, = ops
                args = _assert_callable(fn)
                assert args == [], args

                ops = fn()

        wr_ops = []

        for op in ops:
            wr_ops.append(_wr(op))

        self.ops = wr_ops

        self.__post_init__()

    def __repr__(self) -> str:
        r = ', '.join(str(x) for x in self.ops)
        return f'{self.__class__.__name__}({r})'

    def dependencies(self) -> List['Op']:
        return self.ops

    def execute(self, *args: Any) -> TRes:
        return list(args)


class Arr(Op):
    items: List[Op]

    def __init__(self, *args: Op):
        self.items = args

        self.__post_init__()

    def dependencies(self) -> List['Op']:
        return self.items

    def execute(self, *args: Any) -> TRes:
        return args


class Map(Op):
    """
    Map a sequence to a sequence of new values

    .. code-block:: python
        :linenos:

        Map(
            lambda x: x * x,
            Con([1, 2, 3),
        )
    """

    target: Var
    map: Op
    iter: Op

    def __init__(self, *args: WT):
        if _check_callable(args[0]):
            tar, map, it = self.from_ext(*args)
        else:
            tar, map, it = args

        self.target = _wr(tar)
        self.map = _wr(map)
        self.iter = _wr(it)

        assert isinstance(self.target, Op)
        assert isinstance(self.map, Op)
        assert isinstance(self.iter, Op)

    @classmethod
    def from_ext(cls, map_fn: Callable, it: WT):
        map_arg, = _assert_callable(map_fn)

        target = Var(map_arg)

        map = map_fn(target)

        return target, map, it

    def dependencies(self) -> List['Op']:
        return [self.iter]

    def execute(self, arg) -> TRes:
        return arg

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if not isinstance(result, list):
            raise OpError(self, f'Returned iterable `{result}` is not a list')

        return [
            With(
                self.target,
                Con(x),
                self.map
            )
            for x in result
        ]

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return [
            x for x in post_result
        ]


class Fil(Op):
    """
    Filter a sequence to a sequence of filtered values

    .. code-block:: python
        :linenos:

        Map(
            lambda x: x > 2,
            Con([1, 2, 3),
        )
    """

    target: Var
    filter: Op
    iter: Op

    def __init__(self, *args: WT):
        if _check_callable(args[0]):
            tar, fil, it = self.from_ext(*args)
        else:
            tar, fil, it = args

        self.target = _wr(tar)
        self.filter = _wr(fil)
        self.iter = _wr(it)

        assert isinstance(self.target, Op)
        assert isinstance(self.filter, Op)
        assert isinstance(self.iter, Op)

    @classmethod
    def from_ext(cls, fil: Callable, it: WT):
        fil_arg, = _assert_callable(fil)

        target = Var(fil_arg)

        fil = fil(target)

        return target, fil, it

    def dependencies(self) -> List['Op']:
        return [self.iter]

    def execute(self, arg) -> TRes:
        return arg

    def post_dependencies(self, result: TRes, *pre_result: List[TRes]) -> List['Op']:
        if not isinstance(result, list):
            raise OpError(self, f'Returned iterable `{result}` is not a list')

        return [
            With(
                self.target,
                Con(x),
                Arr(self.target, self.filter)
            )
            for x in result
        ]

    def post_execute(self, execute_ret: TRes, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return [
            x for x, f in post_result if f
        ]


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
