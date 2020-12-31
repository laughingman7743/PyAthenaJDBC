# -*- coding: utf-8 -*-
import functools
import threading
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pandas import DataFrame

    from pyathenajdbc.cursor import Cursor


def as_pandas(cursor: "Cursor", coerce_float: bool = False) -> "DataFrame":
    from pandas import DataFrame

    description = cursor.description
    if not description:
        return DataFrame()
    names = [metadata[0] for metadata in description]
    return DataFrame.from_records(
        cursor.fetchall(), columns=names, coerce_float=coerce_float
    )


def synchronized(wrapped: Callable[..., Any]) -> Any:
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)

    return _wrapper


def attach_thread_to_jvm(wrapped: Callable[..., Any]) -> Any:
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        import jpype

        if not jpype.java.lang.Thread.isAttached():
            jpype.java.lang.Thread.attach()
        return wrapped(*args, **kwargs)

    return _wrapper
