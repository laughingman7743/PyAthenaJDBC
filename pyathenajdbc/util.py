# -*- coding: utf-8 -*-
import functools
import threading


def as_pandas(cursor, coerce_float=False):
    from pandas import DataFrame

    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(
        cursor.fetchall(), columns=names, coerce_float=coerce_float
    )


def synchronized(wrapped):
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)

    return _wrapper


def attach_thread_to_jvm(wrapped):
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        import jpype

        if not jpype.isThreadAttachedToJVM():
            jpype.attachThreadToJVM()
        return wrapped(*args, **kwargs)

    return _wrapper
