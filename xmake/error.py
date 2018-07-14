from typing import Optional, List

import xmake
from xmake.runtime import JobRec


class ExecError(Exception):

    def __init__(self, rec: JobRec, rec_deps: List[JobRec], e: Optional[Exception] = None):
        self.rec = rec
        self.rec_deps = rec_deps
        self.e = e
        super().__init__(rec, rec_deps, e)

    def __str__(self):
        return f'{self.__class__.__name__}("{self.rec.job.filename}:{self.rec.job.lineno}", {self.e}, {self.rec}, {self.rec_deps})'


class OpError(Exception):
    def __init__(self, op: 'xmake.dsl.Op', reason: Optional[str] = None):
        self.op = op
        self.reason = reason
        super().__init__(op, reason)

    def __str__(self):
        return f'{self.__class__.__name__}("{self.op.filename}:{self.op.lineno}", {self.reason}, {self.op})'
