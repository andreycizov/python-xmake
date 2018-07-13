import logging
from typing import TypeVar, List, Any

from dataclasses import dataclass

TRes = TypeVar('TRes')
TPostRes = TypeVar('TPostRes')


class Op:
    @property
    def logger(self):
        return logging.getLogger(self.__module__ + '.' + self.__class__.__name__)

    def dependencies(self) -> List['Op']:
        """
        :return: list of dependencies to execute before ``execute``
        """
        return []

    def execute(self, *args: Any) -> TRes:
        """
        :param args: what is returned by every dependency returned by ``dependencies``
        :return:
        """
        self.logger.debug('%s', args)
        return None

    def post_dependencies(self, result: TRes) -> List['Op']:
        """
        :param result: what is returned by ``execute``
        :return: list of dependencies to execute before ``post_execute``
        """
        return []

    def post_execute(self, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        """

        :param pre_result: what is returned by every dependency returned by ``dependencies``
        :param post_result: what is returned by every dependency returned by ``post_dependencies``
        :return:
        """
        self.logger.getChild('post_execute').debug('%s %s', pre_result, post_result)
        return pre_result[0]


@dataclass()
class LeftRightRes:
    left: Any
    right: Any


@dataclass
class Constant(Op):
    value: Any

    def execute(self, *args: Any) -> TRes:
        return self.value


@dataclass
class DependsOn(Op):
    pre: List[Op]
    post: List[Op]

    def dependencies(self) -> List['Op']:
        return self.pre

    def post_dependencies(self, result: TRes) -> List['Op']:
        return self.post

    def post_execute(self, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return LeftRightRes(pre_result, post_result)


@dataclass
class Iter(Op):
    aggregator: Any
    next_op: Op
    map_op: Op

    def dependencies(self) -> List['Op']:
        # how do we pass an aggregator to the dependency ?
        return [With(Constant(self.aggregator), self.next_op)]

    def execute(self, rtn: Any) -> TRes:
        return rtn

    def post_dependencies(self, result: TRes) -> List['Op']:
        return [With(Constant(result), self.map_op)]

    def post_execute(self, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        next_agg, next_rtn = pre_result[0], post_result[0]

        if next_agg is not None:
            pass


@dataclass
class With(Op):
    value_op: Op
    map_op: Op

    def dependencies(self) -> List['Op']:
        # we might need to pass context here ?
        return [self.value_op]

    def post_dependencies(self, result: TRes) -> List['Op']:
        # are dependencies called with the return value of me ?
        return [self.map_op]

    def post_execute(self, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return post_result[0]


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

    def post_dependencies(self, result: TRes) -> List['Op']:
        if len(self.ops) > 1:
            return [Seq(*self.ops[1:])]
        else:
            return []

    def post_execute(self, pre_result: List[TRes], post_result: List[TPostRes]) -> TPostRes:
        return [pre_result[0]] + (post_result[0] if len(self.ops) > 1 else [])
