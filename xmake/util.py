import inspect
import sys

from attr import dataclass
from typing import Callable, Any, Dict


def _get_outer_frames(frame, context=1, full_impl=True):
    """Get a list of records for a frame and all higher (calling) frames.

    Each record contains a frame object, filename, line number, function
    name, a list of lines of context, and index within the context."""
    # framelist = []
    while frame:
        if full_impl:
            frameinfo = (frame,) + inspect.getframeinfo(frame, context)
            yield inspect.FrameInfo(*frameinfo)
        else:
            yield frame, None

        if frame == frame.f_back:
            raise IndexError

        frame = frame.f_back

    # return framelist


def _get_stack(context=1):
    """Return a list of records for the stack above the caller's frame."""
    return _get_outer_frames(sys._getframe(1), context)


def _iter_idx(iter_obj, nth):
    for i, x in enumerate(iter_obj):
        if i == nth:
            return x


def _get_caller(depth=2, stdlib_impl=True) -> inspect.FrameInfo:
    """Get caller frame"""
    if stdlib_impl:
        caller = inspect.stack()[depth]
    else:
        caller = _iter_idx(_get_stack(), depth)
    return caller


CLOSURE_ATTR = None

if sys.version_info < (3, 0):
    CLOSURE_ATTR = 'func_closure'
else:
    CLOSURE_ATTR = '__closure__'

assert CLOSURE_ATTR is not None


def _make_cell(value):
    """Create a closure cell object from a value"""

    rtn = (lambda x: lambda: x)(value)

    rtn = getattr(rtn, CLOSURE_ATTR)

    assert len(rtn) == 1, rtn

    rtn, = rtn

    return rtn


@dataclass(repr=False)
class EnclosedFree:
    fn: Callable
    clos_globals: Dict[str, Any] = None

    def __repr__(self):
        fn = self.fn.__code__.co_filename if hasattr(self.fn.__code__, 'co_filename') else ''
        ln = self.fn.__code__.co_firstlineno if hasattr(self.fn.__code__, 'co_firstlineno') else ''
        return f'"{fn}:{ln}"'

    @property
    def co_freevars(self):
        return self.fn.__code__.co_freevars

    def __call__(self, *args):
        assert len(args) == len(self.co_freevars), (args, self.co_freevars)

        closure_vars = tuple(_make_cell(arg) for arg in args)

        defaults = None

        new_fn = type(self.fn)(self.fn.__code__, self.clos_globals, self.fn.__name__, defaults, closure_vars)

        return new_fn()


def _enclosed(fn, clos_globals=None):
    """Transform a function referencing closure variables into a callable function with arguments"""

    if clos_globals is None:
        clos_globals = {}

    return EnclosedFree(fn, clos_globals)
