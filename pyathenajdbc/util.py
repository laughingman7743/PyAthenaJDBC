# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import functools
import threading


def as_pandas(cursor, coerce_float=False):
    """
    Return a pandas dataframe as pandas dataframe.

    Args:
        cursor: (todo): write your description
        coerce_float: (todo): write your description
    """
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
        """
        Decorator to wrap a callable.

        Args:
        """
        with _lock:
            return wrapped(*args, **kwargs)

    return _wrapper


def attach_thread_to_jvm(wrapped):
    """
    Decorator for registering jvm jvm jvm.

    Args:
        wrapped: (bool): write your description
    """
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        """
        Decorator to wrap jVM function.

        Args:
        """
        import jpype

        if not jpype.isThreadAttachedToJVM():
            jpype.attachThreadToJVM()
        return wrapped(*args, **kwargs)

    return _wrapper
