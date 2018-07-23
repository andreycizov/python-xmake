import logging

from typing import Optional, List

from xmake.dsl import Op, Ctx
from xmake.runtime import JobRec


def join_with_tabs(fmtds, depth=1):
    return '\n'.join('\n'.join('\t' * depth + y for y in x.split('\n')) for x in fmtds)


def fmt_ctx_val(x):
    x = repr(x)

    if len(x) > 120:
        return x[:115] + '...<PRUNED>'
    else:
        return x


def fmt_ctx(r: Ctx):
    x = [
        f'{k}={fmt_ctx_val(v)}'
        for k, v in r.mappings
    ]
    return join_with_tabs(x)


def fmt_rec_deps(rec_deps: List[JobRec]):
    x = [
        f'{x.ident}, {x.step}, {x.job}' for x in rec_deps
    ]

    return join_with_tabs(x)


def fmt_rec(r: JobRec, deps: List[JobRec]):
    rtn = [
        f'Id: {r.ident}',
        f'Step: {r.step}',
        f'Job: {r.job}',
        f'Ctx:',
        fmt_ctx(r.ctx),
    ]

    if deps:
        rtn += [
            'Deps:',
            fmt_rec_deps(deps)
        ]

    return join_with_tabs(rtn)


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

        fmtds = []

        if loc:
            fmtds += [f'Loc: {loc}']

        if self.e:
            fmtds += [f'Exception: {self.e}']

        if self.rec:
            fmtds += [fmt_rec(self.rec, self.rec_deps)]

        fmtd = join_with_tabs(fmtds=fmtds)

        try:
            return f'{self.__class__.__name__}({fmtd})'
        except:
            logging.getLogger(__name__).exception('a')
            raise
