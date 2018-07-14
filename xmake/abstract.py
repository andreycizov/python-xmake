import inspect
import logging
from typing import TypeVar, List, Tuple, Any, Optional

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


@dataclass()
class Op:
    # todo instead of execute vs post_execute, allow to execute indefinitely until it returns 0 dependencies.
    # todo this allows us to reduce the stack consumption
    # filename: Optional[str] = None
    # lineno: Optional[int] = None

    def __post_init__(self):
        fr = _get_caller(3)
        fn = fr.filename
        ln = fr.lineno

        try:
            self.filename = fn
            self.lineno = ln
        except Exception as e:
            raise ValueError(self, fn)

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


def _get_caller(depth=2):
    caller = inspect.stack()[depth]
    return caller
