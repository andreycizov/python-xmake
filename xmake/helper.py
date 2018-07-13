from typing import Any

from dataclasses import dataclass

from xmake.dsl import Op, TRes


@dataclass(unsafe_hash=True)
class Dummy(Op):
    id: int

    def execute(self, *args: Any) -> TRes:
        return self.id
