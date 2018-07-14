from typing import Optional, List

from xmake.runtime import JobRec


class ExecError(Exception):

    def __init__(self, rec: JobRec, rec_deps: List[JobRec], e: Optional[Exception] = None):
        self.rec = rec
        self.rec_deps = rec_deps
        self.e = e
        super().__init__(rec, rec_deps, e)
