# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import functools
import sys
import threading

from future.utils import reraise

from pyathenajdbc.error import DatabaseError, Error


def as_pandas(cursor):
    from pandas import DataFrame
    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(cursor.fetchall(), columns=names)


def reraise_dbapi_error():
    exc_info = sys.exc_info()
    import jpype
    value = exc_info[1]
    if isinstance(exc_info[1], jpype._jexception.JavaException):
        if issubclass(exc_info[1].__javaclass__, jpype.java.sql.SQLException):
            args = exc_info[1].args
            if args:
                cause = args[0].cause
                if cause:
                    value = cause.getMessage()
                else:
                    value = args[0].getMessage()
            tp = DatabaseError
        else:
            tp = Error
    else:
        tp = exc_info[0]
    reraise(tp, value, exc_info[2])


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
