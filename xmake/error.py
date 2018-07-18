import logging
from typing import Optional, List

from xmake.dsl import Op
from xmake.runtime import JobRec


class ExecError(Exception):

    def __init__(self, rec: JobRec, rec_deps: List[JobRec], e: Optional[Exception] = None):
        self.rec = rec
        self.rec_deps = rec_deps
        self.e = e
        super().__init__(rec, rec_deps, e)

    def __str__(self):
        loc = None
        job = self.rec.job

        if isinstance(self.rec.job, Op):
            loc = self.rec.job._loc

        try:
            return f'{self.__class__.__name__}({loc}, {self.e}, {self.rec}, {self.rec_deps})'
        except:
            logging.getLogger(__name__).exception('a')
            raise


